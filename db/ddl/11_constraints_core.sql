-- Natural keys & checks
ALTER TABLE core.tenants
  ADD CONSTRAINT tenants_name_uk UNIQUE (name);

ALTER TABLE core.users
  ADD CONSTRAINT users_email_ck CHECK (position('@' in email) > 1);

ALTER TABLE core.products
  ADD CONSTRAINT products_sku_ck CHECK (length(trim(sku)) > 0);

-- Composite uniqueness per tenant (case-insensitive)
CREATE UNIQUE INDEX IF NOT EXISTS uk_products_tenant_sku_ci
  ON core.products (tenant_id, lower(sku));

CREATE UNIQUE INDEX IF NOT EXISTS uk_warehouses_tenant_code_ci
  ON core.warehouses (tenant_id, lower(code));

CREATE UNIQUE INDEX IF NOT EXISTS uk_locations_wh_code_ci
  ON core.locations (warehouse_id, lower(code));

CREATE UNIQUE INDEX IF NOT EXISTS uk_lots_tenant_prod_lot
  ON core.lots (tenant_id, product_id, lower(lot_number));

-- Idempotency on ledger
CREATE UNIQUE INDEX IF NOT EXISTS uk_ledger_tenant_op
  ON core.stock_ledger (tenant_id, op_id);

-- Prevent overlapping holds for same (tenant, product, lot, location)
ALTER TABLE core.holds
  ADD CONSTRAINT holds_no_overlap
  EXCLUDE USING gist (
    tenant_id    WITH =,
    product_id   WITH =,
    lot_id       WITH =,
    location_id  WITH =,
    valid_range  WITH &&
  ) DEFERRABLE INITIALLY IMMEDIATE;
COMMENT ON CONSTRAINT holds_no_overlap ON core.holds
  IS 'Exclusion constraint via btree_gist to prevent overlapping reservations for same lot/location.';

-- Helpful checks
ALTER TABLE core.holds
  ADD CONSTRAINT holds_release_after_create_ck
  CHECK (released_at IS NULL OR released_at >= created_at);
