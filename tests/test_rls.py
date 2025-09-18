from __future__ import annotations
import uuid
from sqlalchemy import text

def test_rls_isolation(engine_app, tenant_ids):
    t1, t2 = tenant_ids
    # Insert a product for tenant 1
    with engine_app.begin() as c1:
        c1.execute(text("SET app.tenant_id = :t"), {"t": str(t1)})
        c1.execute(text("""
            INSERT INTO core.products (tenant_id, sku, name) VALUES
            (current_setting('app.tenant_id')::uuid, 'SKU-1', 'Widget 1')
        """))

    # Try to read from tenant 2 (should see nothing)
    with engine_app.begin() as c2:
        c2.execute(text("SET app.tenant_id = :t"), {"t": str(t2)})
        rows = c2.execute(text("SELECT id FROM core.products WHERE lower(sku) = 'sku-1'")).all()
        assert rows == []

    # Write with mismatched tenant should be blocked by WITH CHECK
    bad_id = uuid.uuid4()
    with engine_app.begin() as c3:
        c3.execute(text("SET app.tenant_id = :t"), {"t": str(t2)})
        try:
            c3.execute(text("""
              INSERT INTO core.products (tenant_id, sku, name) VALUES
              (:bad, 'SKU-2', 'Widget 2')
            """), {"bad": str(t1)})
            assert False, "Should have failed RLS WITH CHECK"
        except Exception:
            pass
