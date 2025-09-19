SET search_path = core, public;

-- Tenants
CREATE TABLE IF NOT EXISTS tenants (
  id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name       text NOT NULL,
  is_active  boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now()
);
COMMENT ON TABLE tenants IS 'SaaS tenants. RLS isolates by current_setting(app.tenant_id).';
COMMENT ON COLUMN tenants.name IS 'Natural key per tenant directory; unique globally for demo.';

-- Users (per tenant)
CREATE TABLE IF NOT EXISTS users (
  id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id  uuid NOT NULL REFERENCES core.tenants(id) ON DELETE RESTRICT,
  email      text NOT NULL,
  full_name  text,
  is_active  boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now()
);
COMMENT ON TABLE users IS 'Tenant-scoped app users.';

-- Warehouses
CREATE TABLE IF NOT EXISTS warehouses (
  id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id  uuid NOT NULL REFERENCES core.tenants(id) ON DELETE RESTRICT,
  code       text NOT NULL,
  name       text NOT NULL,
  addr       jsonb NOT NULL DEFAULT '{}'::jsonb,
  is_active  boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now()
);
COMMENT ON TABLE warehouses IS 'Physical warehouses (optionally geocoded later).';

-- Locations (inside a warehouse)
CREATE TABLE IF NOT EXISTS locations (
  id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id     uuid NOT NULL REFERENCES core.tenants(id) ON DELETE RESTRICT,
  warehouse_id  uuid NOT NULL REFERENCES core.warehouses(id) ON DELETE RESTRICT,
  code          text NOT NULL,
  name          text,
  is_active     boolean NOT NULL DEFAULT true,
  created_at    timestamptz NOT NULL DEFAULT now()
);
COMMENT ON TABLE locations IS 'Storage locations per warehouse.';

-- Products
CREATE TABLE IF NOT EXISTS products (
  id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id    uuid NOT NULL REFERENCES core.tenants(id) ON DELETE RESTRICT,
  sku          text NOT NULL,
  name         text NOT NULL,
  description  text,
  attributes   jsonb NOT NULL DEFAULT '{}'::jsonb,
  is_active    boolean NOT NULL DEFAULT true,
  created_at   timestamptz NOT NULL DEFAULT now(),
  -- Generated tsvector (Postgres 12+ supports jsonb_to_tsvector)
  search_tsv   tsvector GENERATED ALWAYS AS (
    setweight(to_tsvector('english', coalesce(name, '')), 'A') ||
    setweight(to_tsvector('english', coalesce(description, '')), 'B') ||
    setweight(jsonb_to_tsvector('english', attributes, '["all"]'), 'C')
  ) STORED
);
COMMENT ON TABLE products IS 'Products with JSONB attributes. FTS via generated tsvector.';

-- Lots (batch/expiry)
CREATE TABLE IF NOT EXISTS lots (
  id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id    uuid NOT NULL REFERENCES core.tenants(id) ON DELETE RESTRICT,
  product_id   uuid NOT NULL REFERENCES core.products(id) ON DELETE RESTRICT,
  lot_number   text NOT NULL,
  expiry_date  date,
  is_active    boolean NOT NULL DEFAULT true,
  created_at   timestamptz NOT NULL DEFAULT now()
);
COMMENT ON TABLE lots IS 'Batch/lot instances for a product (supports FIFO by expiry).';

-- Orders
CREATE TABLE IF NOT EXISTS orders (
  id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id    uuid NOT NULL REFERENCES core.tenants(id) ON DELETE RESTRICT,
  external_ref text,
  status       text NOT NULL DEFAULT 'open' CHECK (status IN ('open','allocated','shipped','cancelled')),
  created_at   timestamptz NOT NULL DEFAULT now()
);
COMMENT ON TABLE orders IS 'Customer orders (minimal for demo).';

-- Order lines
CREATE TABLE IF NOT EXISTS order_lines (
  id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id   uuid NOT NULL REFERENCES core.tenants(id) ON DELETE RESTRICT,
  order_id    uuid NOT NULL REFERENCES core.orders(id) ON DELETE CASCADE,
  product_id  uuid NOT NULL REFERENCES core.products(id) ON DELETE RESTRICT,
  qty         integer NOT NULL CHECK (qty > 0),
  created_at  timestamptz NOT NULL DEFAULT now()
);
COMMENT ON TABLE order_lines IS 'Requested quantities by product.';

-- Holds (reservations)
CREATE TABLE IF NOT EXISTS holds (
  id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id     uuid NOT NULL REFERENCES core.tenants(id) ON DELETE RESTRICT,
  order_id      uuid NOT NULL REFERENCES core.orders(id) ON DELETE CASCADE,
  order_line_id uuid NOT NULL REFERENCES core.order_lines(id) ON DELETE CASCADE,
  product_id    uuid NOT NULL REFERENCES core.products(id) ON DELETE RESTRICT,
  lot_id        uuid NOT NULL REFERENCES core.lots(id) ON DELETE RESTRICT,
  warehouse_id  uuid NOT NULL REFERENCES core.warehouses(id) ON DELETE RESTRICT,
  location_id   uuid NOT NULL REFERENCES core.locations(id) ON DELETE RESTRICT,
  qty           integer NOT NULL CHECK (qty > 0),
  created_at    timestamptz NOT NULL DEFAULT now(),
  released_at   timestamptz,
  valid_range   tstzrange GENERATED ALWAYS AS (tstzrange(created_at, released_at, '[]')) STORED
);
COMMENT ON TABLE holds IS 'Reservations against stock. Exclusion constraint prevents overlapping holds (double-booking).';

-- Stock ledger (partitioned monthly on ts)
CREATE TABLE IF NOT EXISTS stock_ledger (
  id            uuid NOT NULL DEFAULT gen_random_uuid(),
  tenant_id     uuid NOT NULL REFERENCES core.tenants(id) ON DELETE RESTRICT,
  ts            timestamptz NOT NULL DEFAULT now(),
  event_type    text NOT NULL CHECK (event_type IN ('RECEIPT','SHIP','RESERVE','RELEASE','ADJUST_IN','ADJUST_OUT')),
  warehouse_id  uuid REFERENCES core.warehouses(id) ON DELETE RESTRICT,
  location_id   uuid REFERENCES core.locations(id)   ON DELETE RESTRICT,
  product_id    uuid NOT NULL REFERENCES core.products(id) ON DELETE RESTRICT,
  lot_id        uuid REFERENCES core.lots(id) ON DELETE RESTRICT,
  order_id      uuid REFERENCES core.orders(id) ON DELETE SET NULL,
  order_line_id uuid REFERENCES core.order_lines(id) ON DELETE SET NULL,
  qty_delta     integer NOT NULL,
  reason        text,
  op_id         uuid NOT NULL,
  CONSTRAINT pk_stock_ledger PRIMARY KEY (id, ts)
) PARTITION BY RANGE (ts);
COMMENT ON TABLE stock_ledger IS 'Immutable event log. Idempotency via (tenant_id, op_id) unique.';
