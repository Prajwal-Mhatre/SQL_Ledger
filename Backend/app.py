from __future__ import annotations
import json
import os
import uuid
from decimal import Decimal, InvalidOperation
from pathlib import Path
from contextlib import contextmanager
from typing import Any, Dict, Mapping

from flask import Flask, request, jsonify, g, send_from_directory, abort
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.exc import IntegrityError, ProgrammingError
from markupsafe import escape

from backend.services.allocation import allocate_order, release_order
from backend.services.refresh_materialized import refresh_current_stock_mv

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg://osl_app:osl_app@localhost:5432/osl")
DEFAULT_TENANT_ID = os.getenv("DEFAULT_TENANT_ID", "00000000-0000-0000-0000-000000000001")
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN")  # optional; enables /admin routes when set
API_TOKEN = os.getenv("API_TOKEN")  # shared-secret guard for mutating APIs (optional)

ALLOWED_LEDGER_EVENTS = {"RECEIPT", "SHIP", "ADJUST_IN", "ADJUST_OUT"}

app = Flask(__name__, static_folder=None)
engine: Engine = create_engine(DATABASE_URL, future=True, pool_pre_ping=True)


def _load_named_sql(path: Path) -> dict[str, str]:
    content = path.read_text(encoding="utf-8")
    blocks: dict[str, str] = {}
    current: str | None = None
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


SQL_DIR = Path(__file__).resolve().parents[1] / "db" / "queries"
with open(SQL_DIR / "product_search.sql", "r", encoding="utf-8") as f:
    SQL_PRODUCT_SEARCH = text(f.read())
with open(SQL_DIR / "current_stock.sql", "r", encoding="utf-8") as f:
    SQL_CURRENT_STOCK = text(f.read())
DW_SQL = {name: text(stmt) for name, stmt in _load_named_sql(SQL_DIR / "dw_upsert_dims.sql").items()}


def require_tenant() -> uuid.UUID:
    tid = request.headers.get("X-Tenant-Id", DEFAULT_TENANT_ID)
    try:
        return uuid.UUID(tid)
    except Exception as exc:  # pragma: no cover - defensive guard
        raise ValueError("Invalid or missing X-Tenant-Id header") from exc


def require_api_token() -> None:
    if not API_TOKEN:
        return
    if request.headers.get("X-Api-Token") != API_TOKEN:
        abort(401)


@app.before_request
def open_db_conn():
    g.db = engine.connect()  # type: Connection
    g.db.execute(text("SET application_name = 'open-stock-ledger'"))
    g.db.commit()


@app.after_request
def close_db_conn(response):
    conn: Connection | None = getattr(g, "db", None)
    if conn is not None:
        conn.close()
    return response


def set_tenant(conn: Connection, tenant_id: uuid.UUID):
    conn.execute(text("SELECT set_config('app.tenant_id', :tid, false)"), {"tid": str(tenant_id)})


@contextmanager
def tenant_transaction(tenant_id: uuid.UUID):
    conn: Connection = g.db
    if conn.in_transaction():
        conn.rollback()
    with conn.begin():
        set_tenant(conn, tenant_id)
        yield conn


@contextmanager
def simple_transaction():
    conn: Connection = g.db
    if conn.in_transaction():
        conn.rollback()
    with conn.begin():
        yield conn


def _parse_price_cents(value: Any | None) -> int:
    if value is None:
        raise ValueError("price is required")
    try:
        amount = Decimal(str(value)).quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError("price must be numeric") from exc
    cents = int(amount * 100)
    if cents < 0:
        raise ValueError("price must be non-negative")
    return cents


def _constraint_name(err: IntegrityError) -> str:
    orig = getattr(err, "orig", None)
    diag = getattr(orig, "diag", None)
    constraint = getattr(diag, "constraint_name", None) if diag else None
    return constraint or ""


def _programming_error_message(exc: ProgrammingError) -> str:
    orig = getattr(exc, "orig", exc)
    return str(orig)


def _coerce_json_object(value: Any | None, field: str) -> dict:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    raise ValueError(f"{field} must be a JSON object")


def _coerce_attrs(value: Any | None) -> dict:
    return _coerce_json_object(value, "attributes")


def _upsert_dim_product(conn: Connection, tenant_id: uuid.UUID, row: Mapping[str, Any]) -> None:
    attrs_value = row.get("attributes") or {}
    if isinstance(attrs_value, str):
        try:
            attrs_value = json.loads(attrs_value)
        except json.JSONDecodeError:
            attrs_value = {}
    price_cents = int(row.get("price_cents") or 0)
    conn.execute(
        DW_SQL["upsert_dim_product"],
        {
            "tenant_id": str(tenant_id),
            "product_nk": str(row["id"]),
            "sku": row["sku"],
            "name": row["name"],
            "attrs": json.dumps(attrs_value),
            "price_cents": price_cents,
        },
    )


def _upsert_dim_warehouse(conn: Connection, tenant_id: uuid.UUID, row: Mapping[str, Any]) -> None:
    conn.execute(
        DW_SQL["upsert_dim_warehouse"],
        {
            "tenant_id": str(tenant_id),
            "warehouse_nk": str(row["id"]),
            "code": row["code"],
            "name": row["name"],
        },
    )


def _upsert_dim_customer(conn: Connection, tenant_id: uuid.UUID, row: Mapping[str, Any]) -> None:
    conn.execute(
        DW_SQL["upsert_dim_customer"],
        {
            "tenant_id": str(tenant_id),
            "customer_nk": str(row["id"]),
            "name": row["name"],
        },
    )


def _validate_uuid(value: str | None, label: str) -> uuid.UUID:
    if not value:
        raise ValueError(f"{label} is required")
    try:
        return uuid.UUID(str(value))
    except Exception as exc:
        raise ValueError(f"{label} must be a valid UUID") from exc


@app.get("/health")
def health():
    return jsonify({"ok": True})


@app.get("/")
def index_html():
    root = Path(__file__).resolve().parents[1] / "frontend"
    return send_from_directory(root, "index.html")


@app.get("/app.js")
def app_js():
    root = Path(__file__).resolve().parents[1] / "frontend"
    return send_from_directory(root, "app.js")


@app.get("/styles.css")
def styles_css():
    root = Path(__file__).resolve().parents[1] / "frontend"
    return send_from_directory(root, "styles.css")


@app.post("/api/tenants")
def create_tenant():
    require_api_token()
    payload: Dict[str, Any] = request.get_json(force=True, silent=False) or {}
    name = (payload.get("name") or "").strip()
    if not name:
        return jsonify({"error": "name is required"}), 400
    tenant_id = uuid.uuid4()
    try:
        with simple_transaction() as conn:
            set_tenant(conn, tenant_id)
            conn.execute(
                text(
                    """
                    INSERT INTO core.tenants (id, name, is_active)
                    VALUES (current_setting('app.tenant_id')::uuid, :name, true)
                    """
                ),
                {"name": name},
            )
    except IntegrityError as exc:
        if _constraint_name(exc) == "tenants_name_uk":
            return jsonify({"error": "tenant name already exists"}), 409
        raise
    except ProgrammingError as exc:
        message = _programming_error_message(exc).lower()
        if "core.tenants" in message:
            return jsonify({"error": "tenant table is missing. Run database migrations and retry."}), 500
        raise
    return jsonify({"tenant_id": str(tenant_id), "name": name}), 201


@app.post("/api/products")
def create_product():
    require_api_token()
    tenant_id = require_tenant()
    payload: Dict[str, Any] = request.get_json(force=True, silent=False) or {}
    sku = (payload.get("sku") or "").strip()
    name = (payload.get("name") or "").strip()
    description = (payload.get("description") or "").strip() or None
    attrs = _coerce_attrs(payload.get("attributes"))
    try:
        price_cents = _parse_price_cents(payload.get("price"))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    if not sku or not name:
        return jsonify({"error": "sku and name are required"}), 400

    product_id = uuid.uuid4()
    try:
        with tenant_transaction(tenant_id) as conn:
            row = conn.execute(
                text(
                    """
                    INSERT INTO core.products (id, tenant_id, sku, name, description, attributes, price_cents)
                    VALUES (:id, current_setting('app.tenant_id')::uuid, :sku, :name, :description, :attrs, :price_cents)
                    RETURNING id, sku, name, description, attributes, price_cents
                    """
                ),
                {
                    "id": str(product_id),
                    "sku": sku,
                    "name": name,
                    "description": description,
                    "attrs": json.dumps(attrs),
                    "price_cents": price_cents,
                },
            ).mappings().one()
            _upsert_dim_product(conn, tenant_id, row)
    except IntegrityError as exc:
        if _constraint_name(exc) == "uk_products_tenant_sku_ci":
            return jsonify({"error": "sku already exists for this tenant"}), 409
        raise
    except ProgrammingError as exc:
        message = _programming_error_message(exc).lower()
        if "price_cents" in message:
            return jsonify({"error": "core.products.price_cents is missing. Run the latest migrations and retry."}), 500
        if "core.products" in message:
            return jsonify({"error": "core.products table is missing. Run the latest migrations and retry."}), 500
        raise
    row_dict = dict(row)
    row_dict["id"] = str(product_id)
    row_dict["price_cents"] = int(row_dict.get("price_cents", price_cents))
    row_dict["price"] = row_dict["price_cents"] / 100
    if isinstance(row_dict.get("attributes"), str):
        try:
            row_dict["attributes"] = json.loads(row_dict["attributes"])
        except json.JSONDecodeError:
            pass
    return jsonify(row_dict), 201


@app.put("/api/products/<product_id>")
def update_product(product_id: str):
    require_api_token()
    tenant_id = require_tenant()
    payload: Dict[str, Any] = request.get_json(force=True, silent=False) or {}
    updates = []
    params: Dict[str, Any] = {"product_id": product_id}
    if "sku" in payload:
        updates.append("sku = :sku")
        params["sku"] = (payload.get("sku") or "").strip()
    if "name" in payload:
        updates.append("name = :name")
        params["name"] = (payload.get("name") or "").strip()
    if "description" in payload:
        updates.append("description = :description")
        params["description"] = (payload.get("description") or "").strip() or None
    if "attributes" in payload:
        try:
            attrs = _coerce_attrs(payload.get("attributes"))
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        updates.append("attributes = :attrs")
        params["attrs"] = json.dumps(attrs)
    if "price" in payload:
        try:
            params["price_cents"] = _parse_price_cents(payload.get("price"))
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        updates.append("price_cents = :price_cents")

    if not updates:
        return jsonify({"error": "no fields to update"}), 400

    set_clause = ", ".join(updates)
    with tenant_transaction(tenant_id) as conn:
        row = conn.execute(
            text(
                f"""
                UPDATE core.products
                   SET {set_clause}
                 WHERE id = :product_id
                   AND tenant_id = current_setting('app.tenant_id')::uuid
                RETURNING id, sku, name, description, attributes, price_cents
                """
            ),
            params,
        ).mappings().one_or_none()
        if row is None:
            return jsonify({"error": "product not found"}), 404
        _upsert_dim_product(conn, tenant_id, row)
    row_dict = dict(row)
    price_cents_value = int(row_dict["price_cents"])
    row_dict["id"] = str(row_dict["id"])
    row_dict["price_cents"] = price_cents_value
    row_dict["price"] = price_cents_value / 100
    if isinstance(row_dict.get("attributes"), str):
        try:
            row_dict["attributes"] = json.loads(row_dict["attributes"])
        except json.JSONDecodeError:
            pass
    return jsonify(row_dict)


@app.post("/api/warehouses")
def create_warehouse():
    require_api_token()
    tenant_id = require_tenant()
    payload = request.get_json(force=True, silent=False) or {}
    code = (payload.get("code") or "").strip()
    name = (payload.get("name") or "").strip()
    try:
        addr = _coerce_json_object(payload.get("addr"), "addr")
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    if not code or not name:
        return jsonify({"error": "code and name are required"}), 400
    warehouse_id = uuid.uuid4()
    try:
        with tenant_transaction(tenant_id) as conn:
            row = conn.execute(
                text(
                    """
                    INSERT INTO core.warehouses (id, tenant_id, code, name, addr)
                    VALUES (:id, current_setting('app.tenant_id')::uuid, :code, :name, :addr)
                    RETURNING id, code, name
                    """
                ),
                {"id": str(warehouse_id), "code": code, "name": name, "addr": json.dumps(addr)},
            ).mappings().one()
            _upsert_dim_warehouse(conn, tenant_id, row)
    except IntegrityError as exc:
        if _constraint_name(exc) == "uk_warehouses_tenant_code_ci":
            return jsonify({"error": "warehouse code already exists for this tenant"}), 409
        raise
    except ProgrammingError as exc:
        message = _programming_error_message(exc).lower()
        if "core.warehouses" in message:
            return jsonify({"error": "core.warehouses table is missing. Run the latest migrations and retry."}), 500
        raise
    return jsonify({"id": str(warehouse_id), "code": code, "name": name}), 201


@app.put("/api/warehouses/<warehouse_id>")
def update_warehouse(warehouse_id: str):
    require_api_token()
    tenant_id = require_tenant()
    payload = request.get_json(force=True, silent=False) or {}
    updates = []
    params: Dict[str, Any] = {"warehouse_id": warehouse_id}
    if "code" in payload:
        updates.append("code = :code")
        params["code"] = (payload.get("code") or "").strip()
    if "name" in payload:
        updates.append("name = :name")
        params["name"] = (payload.get("name") or "").strip()
    if "addr" in payload:
        try:
            addr_val = _coerce_json_object(payload.get("addr"), "addr")
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        updates.append("addr = :addr")
        params["addr"] = json.dumps(addr_val)
    if not updates:
        return jsonify({"error": "no fields to update"}), 400

    set_clause = ", ".join(updates)
    with tenant_transaction(tenant_id) as conn:
        row = conn.execute(
            text(
                f"""
                UPDATE core.warehouses
                   SET {set_clause}
                 WHERE id = :warehouse_id
                   AND tenant_id = current_setting('app.tenant_id')::uuid
                RETURNING id, code, name
                """
            ),
            params,
        ).mappings().one_or_none()
        if row is None:
            return jsonify({"error": "warehouse not found"}), 404
        _upsert_dim_warehouse(conn, tenant_id, row)
    return jsonify({"id": str(row["id"]), "code": row["code"], "name": row["name"]})


@app.post("/api/customers")
def create_customer():
    require_api_token()
    tenant_id = require_tenant()
    payload = request.get_json(force=True, silent=False) or {}
    code = (payload.get("code") or "").strip()
    name = (payload.get("name") or "").strip()
    email = (payload.get("email") or "").strip() or None
    if not code or not name:
        return jsonify({"error": "code and name are required"}), 400
    customer_id = uuid.uuid4()
    try:
        with tenant_transaction(tenant_id) as conn:
            row = conn.execute(
                text(
                    """
                    INSERT INTO core.customers (id, tenant_id, code, name, email)
                    VALUES (:id, current_setting('app.tenant_id')::uuid, :code, :name, :email)
                    RETURNING id, code, name
                    """
                ),
                {"id": str(customer_id), "code": code, "name": name, "email": email},
            ).mappings().one()
            _upsert_dim_customer(conn, tenant_id, row)
    except IntegrityError as exc:
        if _constraint_name(exc) == "uk_customers_tenant_code_ci":
            return jsonify({"error": "customer code already exists for this tenant"}), 409
        raise
    except ProgrammingError as exc:
        message = _programming_error_message(exc).lower()
        if "core.customers" in message:
            return jsonify({"error": "core.customers table is missing. Run the latest migrations and retry."}), 500
        raise
    return jsonify({"id": str(customer_id), "code": code, "name": name}), 201


@app.put("/api/customers/<customer_id>")
def update_customer(customer_id: str):
    require_api_token()
    tenant_id = require_tenant()
    payload = request.get_json(force=True, silent=False) or {}
    updates = []
    params: Dict[str, Any] = {"customer_id": customer_id}
    if "code" in payload:
        updates.append("code = :code")
        params["code"] = (payload.get("code") or "").strip()
    if "name" in payload:
        updates.append("name = :name")
        params["name"] = (payload.get("name") or "").strip()
    if "email" in payload:
        updates.append("email = :email")
        params["email"] = (payload.get("email") or "").strip() or None
    if not updates:
        return jsonify({"error": "no fields to update"}), 400

    set_clause = ", ".join(updates)
    with tenant_transaction(tenant_id) as conn:
        row = conn.execute(
            text(
                f"""
                UPDATE core.customers
                   SET {set_clause}
                 WHERE id = :customer_id
                   AND tenant_id = current_setting('app.tenant_id')::uuid
                RETURNING id, code, name
                """
            ),
            params,
        ).mappings().one_or_none()
        if row is None:
            return jsonify({"error": "customer not found"}), 404
        _upsert_dim_customer(conn, tenant_id, row)
    return jsonify({"id": str(row["id"]), "code": row["code"], "name": row["name"]})


@app.post("/api/stock_events")
def create_stock_event():
    require_api_token()
    tenant_id = require_tenant()
    payload = request.get_json(force=True, silent=False) or {}
    event_type = (payload.get("event_type") or "").upper()
    if event_type not in ALLOWED_LEDGER_EVENTS:
        return jsonify({"error": "invalid event_type"}), 400
    try:
        qty = int(payload.get("qty", 0))
    except (TypeError, ValueError):
        return jsonify({"error": "qty must be an integer"}), 400
    if qty <= 0:
        return jsonify({"error": "qty must be positive"}), 400

    if event_type in {"RECEIPT", "ADJUST_IN"}:
        qty_delta = qty
    else:
        qty_delta = -qty

    warehouse_id = payload.get("warehouse_id")
    product_id = payload.get("product_id")
    if not warehouse_id or not product_id:
        return jsonify({"error": "warehouse_id and product_id are required"}), 400

    location_id = payload.get("location_id")
    lot_id = payload.get("lot_id")
    order_id = payload.get("order_id")
    order_line_id = payload.get("order_line_id")
    reason = (payload.get("reason") or "").strip() or None
    op_id = payload.get("op_id") or str(uuid.uuid4())
    ts = payload.get("ts")

    with tenant_transaction(tenant_id) as conn:
        inserted = conn.execute(
            text(
                """
                INSERT INTO core.stock_ledger
                  (tenant_id, event_type, warehouse_id, location_id, product_id, lot_id,
                   order_id, order_line_id, qty_delta, reason, op_id, ts)
                VALUES
                  (current_setting('app.tenant_id')::uuid, :event_type, :warehouse_id, :location_id,
                   :product_id, :lot_id, :order_id, :order_line_id, :qty_delta, :reason, :op_id,
                   COALESCE(:ts, now()))
                RETURNING id, ts, qty_delta
                """
            ),
            {
                "event_type": event_type,
                "warehouse_id": warehouse_id,
                "location_id": location_id,
                "product_id": product_id,
                "lot_id": lot_id,
                "order_id": order_id,
                "order_line_id": order_line_id,
                "qty_delta": qty_delta,
                "reason": reason,
                "op_id": op_id,
                "ts": ts,
            },
        ).mappings().one()
        refresh_current_stock_mv(conn)
    return jsonify({"id": str(inserted["id"]), "op_id": op_id, "qty_delta": qty_delta}), 201


@app.get("/api/products")
def product_search():
    tenant_id = require_tenant()
    q = request.args.get("q", "").strip()
    like = f"%{q}%"
    limit = int(request.args.get("limit", "20"))
    offset = int(request.args.get("offset", "0"))

    with tenant_transaction(tenant_id) as conn:
        rows = [dict(row) for row in conn.execute(SQL_PRODUCT_SEARCH, {"q": q, "like": like, "limit": limit, "offset": offset}).mappings()]
    return jsonify({"items": list(rows)})


@app.post("/api/orders")
def create_order():
    tenant_id = require_tenant()
    payload: Dict[str, Any] = request.get_json(force=True, silent=False) or {}
    external_ref = (payload.get("external_ref") or "").strip()
    lines = payload.get("lines") or []
    customer_id = payload.get("customer_id")
    if not lines:
        return jsonify({"error": "lines required"}), 400
    if not customer_id:
        return jsonify({"error": "customer_id required"}), 400

    try:
        customer_uuid = _validate_uuid(customer_id, "customer_id")
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    normalized_lines: list[dict[str, Any]] = []
    for line in lines:
        product_id = line.get("product_id")
        if not product_id:
            return jsonify({"error": "product_id is required"}), 400
        try:
            qty_val = int(line.get("qty", 0))
        except (TypeError, ValueError):
            return jsonify({"error": "qty must be an integer"}), 400
        if qty_val <= 0:
            return jsonify({"error": "qty must be positive"}), 400
        normalized_lines.append({"product_id": product_id, "qty": qty_val})

    with tenant_transaction(tenant_id) as conn:
        exists = conn.execute(
            text(
                "SELECT 1 FROM core.customers WHERE id = :cid AND tenant_id = current_setting('app.tenant_id')::uuid"
            ),
            {"cid": str(customer_uuid)},
        ).scalar_one_or_none()
        if exists is None:
            return jsonify({"error": "customer not found"}), 404

        order_id = uuid.uuid4()
        conn.execute(
            text(
                """
                INSERT INTO core.orders (id, tenant_id, external_ref, status, customer_id)
                VALUES (:id, current_setting('app.tenant_id')::uuid, :ref, 'open', :customer_id)
                """
            ),
            {"id": str(order_id), "ref": external_ref, "customer_id": str(customer_uuid)},
        )
        for line in normalized_lines:
            conn.execute(
                text(
                    """
                    INSERT INTO core.order_lines (tenant_id, order_id, product_id, qty)
                    VALUES (current_setting('app.tenant_id')::uuid, :order_id, :product_id, :qty)
                    """
                ),
                {"order_id": str(order_id), "product_id": line["product_id"], "qty": line["qty"]},
            )
    return jsonify({"order_id": str(order_id)}), 201


@app.post("/api/orders/<order_id>/allocate")
def allocate(order_id: str):
    tenant_id = require_tenant()
    payload = request.get_json(force=True, silent=True) or {}
    try:
        res = allocate_order(engine, tenant_id=uuid.UUID(str(tenant_id)), order_id=uuid.UUID(order_id), request_hint=payload)
        return jsonify(res)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.post("/api/orders/<order_id>/release")
def release(order_id: str):
    tenant_id = require_tenant()
    try:
        res = release_order(engine, tenant_id=uuid.UUID(str(tenant_id)), order_id=uuid.UUID(order_id))
        return jsonify(res)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.get("/api/current_stock")
def current_stock():
    tenant_id = require_tenant()
    product_id = request.args.get("product_id")
    if not product_id:
        return jsonify({"error": "product_id required"}), 400
    with tenant_transaction(tenant_id) as conn:
        rows = [dict(row) for row in conn.execute(SQL_CURRENT_STOCK, {"product_id": product_id}).mappings()]
    return jsonify({"items": list(rows)})


@app.post("/api/refresh_current_stock")
def refresh_mv():
    tenant_id = require_tenant()
    with tenant_transaction(tenant_id) as conn:
        refresh_current_stock_mv(conn)
    return jsonify({"refreshed": True})


@app.post("/admin/refresh_mv")
def admin_refresh_mv():
    if not ADMIN_TOKEN:
        return ("Not Found", 404)
    if request.headers.get("X-Admin-Token") != ADMIN_TOKEN:
        return ("Unauthorized", 401)
    with simple_transaction():
        refresh_current_stock_mv(g.db)
    return jsonify({"refreshed": True})


@app.get("/ui/current_stock_table")
def current_stock_table():
    tenant_id = require_tenant()
    product_id = request.args.get("product_id")
    if not product_id:
        return ("product_id required", 400)

    with tenant_transaction(tenant_id) as conn:
        rows = conn.execute(SQL_CURRENT_STOCK, {"product_id": product_id}).all()

    html = ["<table class='table'><thead><tr><th>Warehouse</th><th>Location</th><th>Lot</th><th>Qty</th></tr></thead><tbody>"]
    for (prod, wh, loc, lot, qty) in rows:
        html.append(
            f"<tr><td>{escape(wh)}</td><td>{escape(loc)}</td><td>{escape(lot or '')}</td><td>{escape(qty)}</td></tr>"
        )
    html.append("</tbody></table>")
    return "".join(html)


if __name__ == "__main__":
    app.run("0.0.0.0", 8000, debug=True)
