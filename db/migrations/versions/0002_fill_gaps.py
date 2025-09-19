from __future__ import annotations
from pathlib import Path
from alembic import op

# revision identifiers, used by Alembic.
revision = "0002_fill_gaps"
down_revision = "0001_init"
branch_labels = None
depends_on = None


def _run_sql(rel_path: str) -> None:
    base = Path(__file__).resolve().parents[2] / "ddl"
    sql_path = base / rel_path
    with open(sql_path, "r", encoding="utf-8") as f:
        op.execute(f.read())


def upgrade() -> None:
    # Product pricing support
    op.execute(
        "ALTER TABLE core.products ADD COLUMN IF NOT EXISTS price_cents integer NOT NULL DEFAULT 0"
    )
    op.execute(
        """
        DO $$
        BEGIN
          IF NOT EXISTS (
            SELECT 1 FROM pg_constraint WHERE conname = 'products_price_cents_ck'
          ) THEN
            ALTER TABLE core.products
              ADD CONSTRAINT products_price_cents_ck CHECK (price_cents >= 0);
          END IF;
        END$$;
        """
    )

    # Customer master data
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS core.customers (
          id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
          tenant_id  uuid NOT NULL REFERENCES core.tenants(id) ON DELETE RESTRICT,
          code       text NOT NULL,
          name       text NOT NULL,
          email      text,
          is_active  boolean NOT NULL DEFAULT true,
          created_at timestamptz NOT NULL DEFAULT now()
        );
        """
    )
    op.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uk_customers_tenant_code_ci ON core.customers (tenant_id, lower(code));"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_customers_active ON core.customers (id) WHERE is_active;"
    )
    op.execute(
        """
        ALTER TABLE core.customers ENABLE ROW LEVEL SECURITY;
        DROP POLICY IF EXISTS customers_rls ON core.customers;
        CREATE POLICY customers_rls ON core.customers
          USING (tenant_id = current_setting('app.tenant_id', true)::uuid)
          WITH CHECK (tenant_id = current_setting('app.tenant_id', true)::uuid);
        """
    )

    # Orders now reference customers
    op.execute(
        "ALTER TABLE core.orders ADD COLUMN IF NOT EXISTS customer_id uuid REFERENCES core.customers(id) ON DELETE SET NULL"
    )
    op.execute(
        "COMMENT ON COLUMN core.orders.customer_id IS 'Links orders to core.customers for analytics and SCD2.'"
    )

    # SCD2 product dimension carries price history
    op.execute(
        "ALTER TABLE dw.dim_product ADD COLUMN IF NOT EXISTS price_cents integer NOT NULL DEFAULT 0"
    )
    op.execute(
        "COMMENT ON COLUMN dw.dim_product.price_cents IS 'Price snapshot (in cents) for SCD2 history.'"
    )

    # Backfill any NULL price_cents values (if column existed but was nullable)
    op.execute("UPDATE dw.dim_product SET price_cents = 0 WHERE price_cents IS NULL")

    # Analytics materialized views (ABC, aging, reorder)
    _run_sql("35_mv_analytics.sql")


def downgrade() -> None:
    # Drop analytics materialized views
    op.execute("DROP MATERIALIZED VIEW IF EXISTS dw.reorder_candidates_mv CASCADE;")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS dw.inventory_aging_mv CASCADE;")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS dw.product_abc_mv CASCADE;")

    # Remove price from dim_product
    op.execute("ALTER TABLE dw.dim_product DROP COLUMN IF EXISTS price_cents")

    # Remove customer linkage on orders
    op.execute("ALTER TABLE core.orders DROP COLUMN IF EXISTS customer_id")

    # Drop customers table and related indexes
    op.execute("DROP TABLE IF EXISTS core.customers CASCADE;")

    # Remove price metadata from core.products
    op.execute("ALTER TABLE core.products DROP CONSTRAINT IF EXISTS products_price_cents_ck")
    op.execute("ALTER TABLE core.products DROP COLUMN IF EXISTS price_cents")
