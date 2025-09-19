SET search_path = dw, public;

-- Products
CREATE TABLE IF NOT EXISTS dim_product (
  product_sk   bigserial PRIMARY KEY,
  tenant_id    uuid NOT NULL,
  product_nk   uuid NOT NULL REFERENCES core.products(id) ON DELETE RESTRICT,
  sku          text NOT NULL,
  name         text NOT NULL,
  attrs        jsonb NOT NULL DEFAULT '{}'::jsonb,
  price_cents  integer NOT NULL DEFAULT 0,
  valid_from   timestamptz NOT NULL DEFAULT now(),
  valid_to     timestamptz,
  is_current   boolean NOT NULL DEFAULT true,
  valid_period tstzrange GENERATED ALWAYS AS (tstzrange(valid_from, valid_to, '[]')) STORED
);
COMMENT ON TABLE dim_product IS 'SCD2 product dimension with non-overlapping validity per (tenant, product_nk).';
COMMENT ON COLUMN dim_product.price_cents IS 'Price snapshot (in cents) for SCD2 history.';

-- No overlap for the same natural key
ALTER TABLE dim_product
  ADD CONSTRAINT dim_product_no_overlap
  EXCLUDE USING gist (tenant_id WITH =, product_nk WITH =, valid_period WITH &&);

-- Fast "current row" lookup
CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_product_current
  ON dim_product (tenant_id, product_nk)
  WHERE is_current;

-- Customers
CREATE TABLE IF NOT EXISTS dim_customer (
  customer_sk  bigserial PRIMARY KEY,
  tenant_id    uuid NOT NULL,
  customer_nk  uuid NOT NULL,
  name         text NOT NULL,
  valid_from   timestamptz NOT NULL DEFAULT now(),
  valid_to     timestamptz,
  is_current   boolean NOT NULL DEFAULT true,
  valid_period tstzrange GENERATED ALWAYS AS (tstzrange(valid_from, valid_to, '[]')) STORED
);
COMMENT ON TABLE dim_customer IS 'SCD2 customer dimension with non-overlapping validity per (tenant, customer_nk).';

ALTER TABLE dim_customer
  ADD CONSTRAINT dim_customer_no_overlap
  EXCLUDE USING gist (tenant_id WITH =, customer_nk WITH =, valid_period WITH &&);

CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_customer_current
  ON dim_customer (tenant_id, customer_nk)
  WHERE is_current;

-- Warehouses
CREATE TABLE IF NOT EXISTS dim_warehouse (
  warehouse_sk bigserial PRIMARY KEY,
  tenant_id    uuid NOT NULL,
  warehouse_nk uuid NOT NULL REFERENCES core.warehouses(id) ON DELETE RESTRICT,
  code         text NOT NULL,
  name         text NOT NULL,
  valid_from   timestamptz NOT NULL DEFAULT now(),
  valid_to     timestamptz,
  is_current   boolean NOT NULL DEFAULT true,
  valid_period tstzrange GENERATED ALWAYS AS (tstzrange(valid_from, valid_to, '[]')) STORED
);
COMMENT ON TABLE dim_warehouse IS 'SCD2 warehouse dimension with non-overlapping validity per (tenant, warehouse_nk).';

ALTER TABLE dim_warehouse
  ADD CONSTRAINT dim_warehouse_no_overlap
  EXCLUDE USING gist (tenant_id WITH =, warehouse_nk WITH =, valid_period WITH &&);

CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_warehouse_current
  ON dim_warehouse (tenant_id, warehouse_nk)
  WHERE is_current;
