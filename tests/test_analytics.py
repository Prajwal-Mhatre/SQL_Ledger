from __future__ import annotations
import uuid
from datetime import datetime, timedelta

from sqlalchemy import text


def _insert_product_bundle(conn, tenant_id, product_id, sku, warehouse_id, location_id, lot_id, price_cents=1000):
    conn.execute(text("SET app.tenant_id = :t"), {"t": str(tenant_id)})
    conn.execute(
        text(
            """
            INSERT INTO core.products (id, tenant_id, sku, name, price_cents)
            VALUES (:id, current_setting('app.tenant_id')::uuid, :sku, :name, :price)
            ON CONFLICT (id) DO NOTHING
            """
        ),
        {"id": str(product_id), "sku": sku, "name": sku, "price": price_cents},
    )
    conn.execute(
        text(
            """
            INSERT INTO core.warehouses (id, tenant_id, code, name)
            VALUES (:id, current_setting('app.tenant_id')::uuid, :code, :name)
            ON CONFLICT (id) DO NOTHING
            """
        ),
        {"id": str(warehouse_id), "code": f"WH-{sku}", "name": f"Warehouse {sku}"},
    )
    conn.execute(
        text(
            """
            INSERT INTO core.locations (id, tenant_id, warehouse_id, code, name)
            VALUES (:id, current_setting('app.tenant_id')::uuid, :wh, :code, :name)
            ON CONFLICT (id) DO NOTHING
            """
        ),
        {"id": str(location_id), "wh": str(warehouse_id), "code": f"LOC-{sku}", "name": f"Location {sku}"},
    )
    conn.execute(
        text(
            """
            INSERT INTO core.lots (id, tenant_id, product_id, lot_number)
            VALUES (:id, current_setting('app.tenant_id')::uuid, :prod, :lot)
            ON CONFLICT (id) DO NOTHING
            """
        ),
        {"id": str(lot_id), "prod": str(product_id), "lot": f"LOT-{sku}"},
    )


def _insert_ledger(conn, tenant_id, event_type, product_id, warehouse_id, location_id, lot_id, qty, ts):
    conn.execute(
        text(
            """
            INSERT INTO core.stock_ledger
              (tenant_id, event_type, warehouse_id, location_id, product_id, lot_id, qty_delta, ts, op_id)
            VALUES (current_setting('app.tenant_id')::uuid, :evt, :wh, :loc, :prod, :lot, :delta, :ts, :op)
            """
        ),
        {
            "evt": event_type,
            "wh": str(warehouse_id),
            "loc": str(location_id),
            "prod": str(product_id),
            "lot": str(lot_id),
            "delta": qty,
            "ts": ts,
            "op": str(uuid.uuid4()),
        },
    )


def test_analytics_materialized_views(engine_app, tenant_ids):
    tenant_id, _ = tenant_ids
    now = datetime.utcnow()

    prod_a = uuid.uuid4()
    prod_b = uuid.uuid4()
    prod_c = uuid.uuid4()

    wh = uuid.uuid4()
    loc = uuid.uuid4()

    lot_a = uuid.uuid4()
    lot_b = uuid.uuid4()
    lot_c = uuid.uuid4()

    with engine_app.begin() as conn:
        _insert_product_bundle(conn, tenant_id, prod_a, "SKU-A", wh, loc, lot_a)
        _insert_product_bundle(conn, tenant_id, prod_b, "SKU-B", wh, loc, lot_b)
        _insert_product_bundle(conn, tenant_id, prod_c, "SKU-C", wh, loc, lot_c)

        conn.execute(text("SET app.tenant_id = :t"), {"t": str(tenant_id)})

        # Product A: receive 90 units 50 days ago, ship 60 units 5 days ago -> on-hand 30
        _insert_ledger(conn, tenant_id, "RECEIPT", prod_a, wh, loc, lot_a, 90, now - timedelta(days=50))
        _insert_ledger(conn, tenant_id, "SHIP", prod_a, wh, loc, lot_a, -60, now - timedelta(days=5))

        # Product B: receive 40 units 20 days ago, ship 25 units 2 days ago -> on-hand 15
        _insert_ledger(conn, tenant_id, "RECEIPT", prod_b, wh, loc, lot_b, 40, now - timedelta(days=20))
        _insert_ledger(conn, tenant_id, "SHIP", prod_b, wh, loc, lot_b, -25, now - timedelta(days=2))

        # Product C: receive 16 units 10 days ago, ship 15 units 1 day ago -> on-hand 1 (reorder candidate)
        _insert_ledger(conn, tenant_id, "RECEIPT", prod_c, wh, loc, lot_c, 16, now - timedelta(days=10))
        _insert_ledger(conn, tenant_id, "SHIP", prod_c, wh, loc, lot_c, -15, now - timedelta(days=1))

        # Refresh derived views
        conn.execute(text("REFRESH MATERIALIZED VIEW dw.current_stock_mv"))
        conn.execute(text("REFRESH MATERIALIZED VIEW dw.product_abc_mv"))
        conn.execute(text("REFRESH MATERIALIZED VIEW dw.inventory_aging_mv"))
        conn.execute(text("REFRESH MATERIALIZED VIEW dw.reorder_candidates_mv"))

        abc_rows = conn.execute(
            text(
                """
                SELECT product_id, abc_class
                  FROM dw.product_abc_mv
                 WHERE tenant_id = current_setting('app.tenant_id')::uuid
                """
            )
        ).mappings().all()
        abc_map = {uuid.UUID(str(row["product_id"])): row["abc_class"] for row in abc_rows}
        assert abc_map[prod_a] == 'A'
        assert abc_map[prod_b] == 'B'
        assert abc_map[prod_c] == 'C'

        aging_row = conn.execute(
            text(
                """
                SELECT age_bucket
                  FROM dw.inventory_aging_mv
                 WHERE tenant_id = current_setting('app.tenant_id')::uuid
                   AND product_id = :prod AND lot_id = :lot
                """
            ),
            {"prod": str(prod_a), "lot": str(lot_a)},
        ).scalar_one()
        assert aging_row in {'30-59', '60-89'}  # depending on clock skew

        reorder = conn.execute(
            text(
                """
                SELECT on_hand, reorder_point, needs_reorder
                  FROM dw.reorder_candidates_mv
                 WHERE tenant_id = current_setting('app.tenant_id')::uuid
                   AND product_id = :prod
                """
            ),
            {"prod": str(prod_c)},
        ).mappings().one()
        assert reorder["on_hand"] == 1
        assert reorder["needs_reorder"] is True
        assert reorder["reorder_point"] >= 4
