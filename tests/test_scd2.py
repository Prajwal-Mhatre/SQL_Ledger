from __future__ import annotations
import uuid
from pathlib import Path
from sqlalchemy import text

def _load_named_sql(path: Path) -> dict[str, str]:
    content = path.read_text(encoding="utf-8")
    blocks = {}
    current = None
    buf = []
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

SQL = _load_named_sql(Path(__file__).resolve().parents[1] / "db" / "queries" / "dw_upsert_dims.sql")

def test_scd2_product_upsert(engine_app, tenant_ids):
    t1, _ = tenant_ids
    prod = uuid.uuid4()

    with engine_app.begin() as c:
        c.execute(text("SET app.tenant_id = :t"), {"t": str(t1)})
        # Seed a core product row (nk reference)
        c.execute(text("""
            INSERT INTO core.products (id, tenant_id, sku, name, attributes)
            VALUES (:id, current_setting('app.tenant_id')::uuid, 'SKU-SCD', 'Name A', '{"color":"red"}')
        """), {"id": str(prod)})

        # First upsert -> inserts current
        r1 = c.execute(text(SQL["upsert_dim_product"]), {
            "tenant_id": str(t1), "product_nk": str(prod),
            "sku": "SKU-SCD", "name": "Name A", "attrs": '{"color":"red"}'
        }).scalar_one()
        assert r1 is not None

        # Second upsert with same attrs -> no new row
        r2 = c.execute(text(SQL["upsert_dim_product"]), {
            "tenant_id": str(t1), "product_nk": str(prod),
            "sku": "SKU-SCD", "name": "Name A", "attrs": '{"color":"red"}'
        }).fetchall()
        # Depending on driver, no RETURNING rows => empty list
        assert r2 == []

        # Third upsert with change -> closes current + inserts new
        r3 = c.execute(text(SQL["upsert_dim_product"]), {
            "tenant_id": str(t1), "product_nk": str(prod),
            "sku": "SKU-SCD", "name": "Name B", "attrs": '{"color":"blue"}'
        }).scalar_one()
        assert r3 is not None

        # Validate: exactly one current row, previous has valid_to set; no overlaps
        cur_count = c.execute(text("""
            SELECT count(*) FROM dw.dim_product
            WHERE tenant_id = current_setting('app.tenant_id')::uuid
              AND product_nk = :p AND is_current
        """), {"p": str(prod)}).scalar_one()
        assert cur_count == 1

        rows = c.execute(text("""
            SELECT product_sk, valid_from, valid_to, is_current
            FROM dw.dim_product
            WHERE tenant_id = current_setting('app.tenant_id')::uuid
              AND product_nk = :p
            ORDER BY product_sk
        """), {"p": str(prod)}).all()
        assert len(rows) == 2
        # earlier version should be closed
        assert rows[0][2] is not None and rows[0][3] is False
        # latest version current
        assert rows[1][2] is None and rows[1][3] is True
