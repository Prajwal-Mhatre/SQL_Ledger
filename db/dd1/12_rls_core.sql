-- Functional indexes
CREATE INDEX IF NOT EXISTS ix_products_sku_lower ON core.products (lower(sku));

-- Partial indexes (active rows)
CREATE INDEX IF NOT EXISTS ix_products_active ON core.products (id) WHERE is_active;
CREATE INDEX IF NOT EXISTS ix_users_active    ON core.users (id) WHERE is_active;
CREATE INDEX IF NOT EXISTS ix_wh_active       ON core.warehouses (id) WHERE is_active;
CREATE INDEX IF NOT EXISTS ix_loc_active      ON core.locations (id) WHERE is_active;
CREATE INDEX IF NOT EXISTS ix_lots_active     ON core.lots (id) WHERE is_active;

-- JSONB GIN on product attributes
CREATE INDEX IF NOT EXISTS ix_products_attrs_gin
  ON core.products USING GIN (attributes jsonb_path_ops);

-- Full-text search GIN on generated column
CREATE INDEX IF NOT EXISTS ix_products_search_tsv
  ON core.products USING GIN (search_tsv);

-- Optional fuzzy search
CREATE INDEX IF NOT EXISTS ix_products_name_trgm
  ON core.products USING GIN (name gin_trgm_ops);

-- Active holds fast lookup
CREATE INDEX IF NOT EXISTS ix_holds_active_prod_lot
  ON core.holds (product_id, lot_id)
  WHERE released_at IS NULL;
