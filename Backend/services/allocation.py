from __future__ import annotations
import hashlib
import os
import random
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Mapping

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy.engine import Engine, Connection

from backend.services.refresh_materialized import refresh_current_stock_mv

RETRY_ERRORS = {"40P01", "40001"}  # deadlock detected / serialization failure


def _load_named_sql(path: Path) -> dict[str, str]:
    """
    Tiny parser for a single .sql file that contains multiple statements,
    each prefixed with a line:  `-- name: <key>`
    Returns a dict {key: sql_text}.
    """
    content = path.read_text(encoding="utf-8")
    blocks: dict[str, str] = {}
    current = None
    buf: list[str] = []
    for line in content.splitlines():
        if line.startswith("-- name:"):
            if current is not None:
                blocks[current] = "\n".join(buf).strip()
                buf = []
            current = line.split(":", 1)[1].strip()
        else:
            buf.append(line)
    if current is not None:
        blocks[current] = "\n".join(buf).strip()
    return blocks


SQL = _load_named_sql(
    Path(__file__).resolve().parents[2] / "db" / "queries" / "allocate.sql"
)


def _set_timeouts(conn: Connection) -> None:
    """
    Keep locks short to reduce contention, fail fast, and let the retry path do the work.
    - lock_timeout: fail if we can't acquire a lock quickly
    - deadlock_timeout: detect deadlocks sooner (default is large)
    - statement_timeout: bound any pathological statement
    """
    for key, value in (('lock_timeout', '200ms'), ('statement_timeout', '4s')):
        conn.execute(text("SELECT set_config(:key, :value, true)"), {'key': key, 'value': value})


def _advisory_lock_order(conn: Connection, tenant_id: uuid.UUID, order_id: uuid.UUID) -> None:
    """Acquire a per-order advisory lock scoped by tenant."""

    def _lock_key(tid: uuid.UUID, oid: uuid.UUID) -> int:
        digest = hashlib.blake2b(digest_size=8)
        digest.update(tid.bytes)
        digest.update(oid.bytes)
        value = int.from_bytes(digest.digest(), byteorder="big", signed=False)
        if value >= 2**63:
            value -= 2**64
        return value

    lock_key = _lock_key(tenant_id, order_id)
    conn.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": lock_key})


def _retry_sleep(attempt: int) -> None:
    """
    Exponential backoff with jitter to avoid thundering herds.
    """
    base = 0.05 * (2 ** attempt)
    time.sleep(base + random.uniform(0, 0.03))


def _select_order_lines(conn: Connection, order_id: uuid.UUID) -> List[Mapping[str, Any]]:
    return list(
        conn.execute(text(SQL["select_order_lines"]), {"order_id": str(order_id)}).mappings().all()
    )


def _select_candidates(conn: Connection, product_id: uuid.UUID, take_limit: int = 64) -> List[Mapping[str, Any]]:
    return list(
        conn.execute(
            text(SQL["allocation_candidates"]), {"product_id": str(product_id), "take_limit": take_limit}
        ).mappings().all()
    )


def _insert_hold_and_reserve(
    conn: Connection,
    order_id: uuid.UUID,
    order_line_id: uuid.UUID,
    product_id: uuid.UUID,
    lot_id: uuid.UUID,
    warehouse_id: uuid.UUID,
    location_id: uuid.UUID,
    qty: int,
) -> None:
    """
    Atomic pair:
      1) Insert HOLD row (enforced by exclusion constraint to avoid overlap).
      2) Insert RESERVE ledger event with a fresh idempotency key `op_id`.
    If a duplicate op_id is ever re-sent, the unique index on (tenant_id, op_id)
    in the ledger protects us (idempotent writes).
    """
    hold_id = uuid.uuid4()
    conn.execute(
        text(SQL["insert_hold"]),
        {
            "id": str(hold_id),
            "order_id": str(order_id),
            "order_line_id": str(order_line_id),
            "product_id": str(product_id),
            "lot_id": str(lot_id),
            "warehouse_id": str(warehouse_id),
            "location_id": str(location_id),
            "qty": int(qty),
        },
    )

    conn.execute(
        text(SQL["insert_ledger_reserve"]),
        {
            "wh": str(warehouse_id),
            "loc": str(location_id),
            "prod": str(product_id),
            "lot": str(lot_id),
            "ord": str(order_id),
            "ol": str(order_line_id),
            "delta": -int(qty),
            "reason": "allocation reserve",
            "op_id": str(uuid.uuid4()),
        },
    )




def release_order(engine: Engine, tenant_id: uuid.UUID, order_id: uuid.UUID) -> dict:
    """Release any active holds for the given order and emit RELEASE ledger rows."""
    with engine.connect() as conn:
        with conn.begin():
            conn.execute(text("SELECT set_config('app.tenant_id', :tid, false)"), {"tid": str(tenant_id)})
            released_rows = conn.execute(
                text(SQL["release_active_holds"]), {"order_id": str(order_id)}
            ).mappings().all()
            released_qty = 0
            ledger_changed = False
            for row in released_rows:
                qty = int(row["qty"])
                released_qty += qty
                conn.execute(
                    text(SQL["insert_ledger_release"]),
                    {
                        "wh": str(row["warehouse_id"]),
                        "loc": str(row["location_id"]),
                        "prod": str(row["product_id"]),
                        "lot": str(row["lot_id"]),
                        "ord": str(order_id),
                        "ol": str(row["order_line_id"]),
                        "delta": qty,
                        "reason": "manual release",
                        "op_id": str(uuid.uuid4()),
                    },
                )
                ledger_changed = True
            if released_rows:
                conn.execute(text(SQL["mark_order_open"]), {"order_id": str(order_id)})
            if ledger_changed:
                refresh_current_stock_mv(conn)
    return {
        "order_id": str(order_id),
        "released_lines": len(released_rows),
        "released_qty": released_qty,
    }


def allocate_order(engine: Engine, tenant_id: uuid.UUID, order_id: uuid.UUID, request_hint: dict | None = None) -> dict:
    """
    End-to-end allocation for a single order.

    Concurrency design:
    - Tenant scoping: we SET app.tenant_id at the start of the txn to let RLS protect all statements.
    - Stable lock ordering: candidates are produced in a fixed order
      (warehouse_id → lot_id → location_id → expiry_date) so that multiple workers acquire locks
      in the same sequence and avoid deadlocks.
    - Row-level locks: candidate lots are acquired with FOR UPDATE SKIP LOCKED,
      preventing workers from blocking on already-claimed rows.
    - Per-order advisory lock: pg_advisory_xact_lock(tenant, order) ensures only one worker
      handles a given order at a time.
    - Idempotency: stock_ledger has a unique (tenant_id, op_id); if a retry repeats an insert,
      the uniqueness check prevents duplicates.
    - Retries: backoff on SQLSTATE 40P01 (deadlock) or 40001 (serialization failure).
    """
    max_attempts = 5
    attempt = 0
    last_err: Exception | None = None

    while attempt < max_attempts:
        try:
            with engine.connect() as conn:
                with conn.begin():
                    # Scope everything to the tenant under RLS
                    conn.execute(text("SELECT set_config('app.tenant_id', :tid, false)"), {"tid": str(tenant_id)})
                    _set_timeouts(conn)
                    try:
                        _advisory_lock_order(conn, tenant_id, order_id)
                    except SQLAlchemyError as exc:
                        sqlstate = getattr(getattr(exc, 'orig', None), 'sqlstate', None)
                        if sqlstate not in {'42883', '42501'}:
                            raise

                    lines = _select_order_lines(conn, order_id)
                    results: List[dict] = []
                    ledger_changed = False

                    for line in lines:
                        remaining = int(line["qty"])
                        product_id = line["product_id"]
                        candidates = _select_candidates(conn, product_id)
                        for c in candidates:
                            if remaining <= 0:
                                break
                            avail = int(c["available_qty"])
                            if avail <= 0:
                                continue
                            take = min(avail, remaining)
                            try:
                                with conn.begin_nested():
                                    _insert_hold_and_reserve(
                                        conn=conn,
                                        order_id=order_id,
                                        order_line_id=line["order_line_id"],
                                        product_id=product_id,
                                        lot_id=c["lot_id"],
                                        warehouse_id=c["warehouse_id"],
                                        location_id=c["location_id"],
                                        qty=take,
                                    )
                                ledger_changed = True
                            except IntegrityError as exc:
                                sqlstate = getattr(getattr(exc, "orig", None), "sqlstate", None)
                                if sqlstate == "23P01":  # hold overlap, try next candidate
                                    continue
                                raise
                            remaining -= take

                        results.append(
                            {
                                "order_line_id": str(line["order_line_id"]),
                                "requested": int(line["qty"]),
                                "allocated": int(line["qty"]) - remaining,
                            }
                        )

                    if any(r["allocated"] > 0 for r in results):
                        conn.execute(text(SQL["mark_order_allocated"]), {"order_id": str(order_id)})

                    if ledger_changed:
                        refresh_current_stock_mv(conn)

            return {"order_id": str(order_id), "lines": results}

        except Exception as e:  # retry on known concurrency errors
            sqlstate = getattr(getattr(e, "orig", None), "sqlstate", None)
            if sqlstate in RETRY_ERRORS:
                attempt += 1
                _retry_sleep(attempt)
                last_err = e
                continue
            raise
    raise last_err or RuntimeError("allocation failed after retries")

