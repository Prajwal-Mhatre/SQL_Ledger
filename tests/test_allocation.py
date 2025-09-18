from __future__ import annotations
import threading
import uuid
from sqlalchemy import text
from backend.services.allocation import allocate_order

def setup_stock(c, tenant, product_id, warehouse_id, location_id, lot_id, qty):
    c.execute(text("SET app.tenant_id = :t"), {"t": str(tenant)})
    c.execute(text("INSERT INTO core.products (id, tenant_id, sku, name) VALUES (:id, current_setting('app.tenant_id')::uuid, 'SKU-A', 'Alloc')"), {"id": str(product_id)})
    c.execute(text("INSERT INTO core.warehouses (id, tenant_id, code, name) VALUES (:id, current_setting('app.tenant_id')::uuid, 'WA','WA')"), {"id": str(warehouse_id)})
    c.execute(text("INSERT INTO core.locations (id, tenant_id, warehouse_id, code, name) VALUES (:id, current_setting('app.tenant_id')::uuid, :wh, 'LA','LA')"), {"id": str(location_id), "wh": str(warehouse_id)})
    c.execute(text("INSERT INTO core.lots (id, tenant_id, product_id, lot_number) VALUES (:id, current_setting('app.tenant_id')::uuid, :p, 'LOT-A')"), {"id": str(lot_id), "p": str(product_id)})
    c.execute(text("""
        INSERT INTO core.stock_ledger
        (tenant_id, event_type, warehouse_id, location_id, product_id, lot_id, qty_delta, op_id)
        VALUES (current_setting('app.tenant_id')::uuid, 'RECEIPT', :wh, :loc, :prod, :lot, :delta, gen_random_uuid())
    """), {"wh": str(warehouse_id), "loc": str(location_id), "prod": str(product_id), "lot": str(lot_id), "delta": qty})

def create_order(c, tenant, product_id, qty):
    oid = uuid.uuid4()
    c.execute(text("SET app.tenant_id = :t"), {"t": str(tenant)})
    c.execute(text("INSERT INTO core.orders (id, tenant_id, status) VALUES (:id, current_setting('app.tenant_id')::uuid, 'open')"), {"id": str(oid)})
    c.execute(text("""
        INSERT INTO core.order_lines (tenant_id, order_id, product_id, qty)
        VALUES (current_setting('app.tenant_id')::uuid, :o, :p, :q)
    """), {"o": str(oid), "p": str(product_id), "q": qty})
    return oid

def test_concurrent_allocators_no_double_book(engine_app, tenant_ids):
    t1, _ = tenant_ids
    prod = uuid.uuid4()
    wh = uuid.uuid4(); loc = uuid.uuid4(); lot = uuid.uuid4()

    with engine_app.begin() as c:
        setup_stock(c, t1, prod, wh, loc, lot, 10)

    # Create two orders for same product
    with engine_app.begin() as c:
        o1 = create_order(c, t1, prod, 6)
        o2 = create_order(c, t1, prod, 6)

    results = {}

    def worker(order_id, key):
        res = allocate_order(engine_app, tenant_id=t1, order_id=order_id, request_hint={})
        results[key] = res

    tA = threading.Thread(target=worker, args=(o1, "a"))
    tB = threading.Thread(target=worker, args=(o2, "b"))
    tA.start(); tB.start()
    tA.join(); tB.join()

    a_alloc = results["a"]["lines"][0]["allocated"]
    b_alloc = results["b"]["lines"][0]["allocated"]

    assert 0 <= a_alloc <= 6
    assert 0 <= b_alloc <= 6
    assert (a_alloc + b_alloc) <= 10  # no double booking

    # Also verify holds sum equals RESERVE in ledger
    with engine_app.begin() as c:
        c.execute(text("SET app.tenant_id = :t"), {"t": str(t1)})
        holds_sum = c.execute(text("""
            SELECT COALESCE(SUM(qty),0) FROM core.holds 
            WHERE tenant_id = current_setting('app.tenant_id')::uuid AND product_id = :p AND released_at IS NULL
        """), {"p": str(prod)}).scalar_one()
        reserves = c.execute(text("""
            SELECT -COALESCE(SUM(qty_delta),0) FROM core.stock_ledger
            WHERE tenant_id = current_setting('app.tenant_id')::uuid AND product_id = :p AND event_type='RESERVE'
        """), {"p": str(prod)}).scalar_one()

        assert holds_sum == reserves
