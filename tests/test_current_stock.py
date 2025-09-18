from __future__ import annotations
import uuid
from sqlalchemy import text

def test_current_stock_matches_mv(engine_app, tenant_ids):
    t1, _ = tenant_ids
    prod = uuid.uuid4()
    wh = uuid.uuid4()
    loc = uuid.uuid4()
    lot = uuid.uuid4()

    with engine_app.begin() as c:
        c.execute(text("SET app.tenant_id = :t"), {"t": str(t1)})
        # Minimal referents
        c.execute(text("INSERT INTO core.products (id, tenant_id, sku, name) VALUES (:id, current_setting('app.tenant_id')::uuid, 'SKU-CS', 'Thing')"), {"id": str(prod)})
        c.execute(text("INSERT INTO core.warehouses (id, tenant_id, code, name) VALUES (:id, current_setting('app.tenant_id')::uuid, 'W1','W1')"), {"id": str(wh)})
        c.execute(text("INSERT INTO core.locations (id, tenant_id, warehouse_id, code, name) VALUES (:id, current_setting('app.tenant_id')::uuid, :wh, 'L1','L1')"), {"id": str(loc), "wh": str(wh)})
        c.execute(text("INSERT INTO core.lots (id, tenant_id, product_id, lot_number) VALUES (:id, current_setting('app.tenant_id')::uuid, :p, 'LOT1')"), {"id": str(lot), "p": str(prod)})

        # Ledger: +10 receipt, -3 ship, +2 adjust_in  => net 9
        for evt, delta in [('RECEIPT', 10), ('SHIP', -3), ('ADJUST_IN', 2)]:
            c.execute(text("""
                INSERT INTO core.stock_ledger
                (tenant_id, event_type, warehouse_id, location_id, product_id, lot_id, qty_delta, op_id)
                VALUES (current_setting('app.tenant_id')::uuid, :evt, :wh, :loc, :prod, :lot, :delta, gen_random_uuid())
            """), {"evt": evt, "wh": str(wh), "loc": str(loc), "prod": str(prod), "lot": str(lot), "delta": delta})

        # Refresh MV
        c.execute(text("REFRESH MATERIALIZED VIEW dw.current_stock_mv"))

        # Compare
        mv = c.execute(text("""
           SELECT qty FROM dw.current_stock_mv
           WHERE tenant_id = current_setting('app.tenant_id')::uuid
             AND product_id = :p AND lot_id = :lot AND warehouse_id = :wh AND location_id = :loc
        """), {"p": str(prod), "lot": str(lot), "wh": str(wh), "loc": str(loc)}).scalar_one()

        sum_ledger = c.execute(text("""
           SELECT COALESCE(SUM(qty_delta),0) FROM core.stock_ledger
           WHERE tenant_id = current_setting('app.tenant_id')::uuid
             AND product_id = :p AND lot_id = :lot AND warehouse_id = :wh AND location_id = :loc
        """), {"p": str(prod), "lot": str(lot), "wh": str(wh), "loc": str(loc)}).scalar_one()

        assert mv == sum_ledger == 9
