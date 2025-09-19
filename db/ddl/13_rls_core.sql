-- Helper expression
-- Using current_setting('app.tenant_id', true) returns NULL if not set; cast to uuid
DO $$
BEGIN
  -- Tenants
  ALTER TABLE core.tenants ENABLE ROW LEVEL SECURITY;
  DROP POLICY IF EXISTS tenants_rls ON core.tenants;
  CREATE POLICY tenants_rls ON core.tenants
    USING (id = current_setting('app.tenant_id', true)::uuid)
    WITH CHECK (id = current_setting('app.tenant_id', true)::uuid);

  -- Users
  ALTER TABLE core.users ENABLE ROW LEVEL SECURITY;
  DROP POLICY IF EXISTS users_rls ON core.users;
  CREATE POLICY users_rls ON core.users
    USING (tenant_id = current_setting('app.tenant_id', true)::uuid)
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true)::uuid);

  -- Customers
  ALTER TABLE core.customers ENABLE ROW LEVEL SECURITY;
  DROP POLICY IF EXISTS customers_rls ON core.customers;
  CREATE POLICY customers_rls ON core.customers
    USING (tenant_id = current_setting('app.tenant_id', true)::uuid)
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true)::uuid);

  -- Warehouses
  ALTER TABLE core.warehouses ENABLE ROW LEVEL SECURITY;
  DROP POLICY IF EXISTS warehouses_rls ON core.warehouses;
  CREATE POLICY warehouses_rls ON core.warehouses
    USING (tenant_id = current_setting('app.tenant_id', true)::uuid)
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true)::uuid);

  -- Locations
  ALTER TABLE core.locations ENABLE ROW LEVEL SECURITY;
  DROP POLICY IF EXISTS locations_rls ON core.locations;
  CREATE POLICY locations_rls ON core.locations
    USING (tenant_id = current_setting('app.tenant_id', true)::uuid)
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true)::uuid);

  -- Products
  ALTER TABLE core.products ENABLE ROW LEVEL SECURITY;
  DROP POLICY IF EXISTS products_rls ON core.products;
  CREATE POLICY products_rls ON core.products
    USING (tenant_id = current_setting('app.tenant_id', true)::uuid)
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true)::uuid);

  -- Lots
  ALTER TABLE core.lots ENABLE ROW LEVEL SECURITY;
  DROP POLICY IF EXISTS lots_rls ON core.lots;
  CREATE POLICY lots_rls ON core.lots
    USING (tenant_id = current_setting('app.tenant_id', true)::uuid)
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true)::uuid);

  -- Orders
  ALTER TABLE core.orders ENABLE ROW LEVEL SECURITY;
  DROP POLICY IF EXISTS orders_rls ON core.orders;
  CREATE POLICY orders_rls ON core.orders
    USING (tenant_id = current_setting('app.tenant_id', true)::uuid)
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true)::uuid);

  -- Order lines
  ALTER TABLE core.order_lines ENABLE ROW LEVEL SECURITY;
  DROP POLICY IF EXISTS order_lines_rls ON core.order_lines;
  CREATE POLICY order_lines_rls ON core.order_lines
    USING (tenant_id = current_setting('app.tenant_id', true)::uuid)
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true)::uuid);

  -- Holds
  ALTER TABLE core.holds ENABLE ROW LEVEL SECURITY;
  DROP POLICY IF EXISTS holds_rls ON core.holds;
  CREATE POLICY holds_rls ON core.holds
    USING (tenant_id = current_setting('app.tenant_id', true)::uuid)
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true)::uuid);

  -- Stock ledger
  ALTER TABLE core.stock_ledger ENABLE ROW LEVEL SECURITY;
  DROP POLICY IF EXISTS ledger_rls ON core.stock_ledger;
  CREATE POLICY ledger_rls ON core.stock_ledger
    USING (tenant_id = current_setting('app.tenant_id', true)::uuid)
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true)::uuid);
END$$;

-- Grant table privileges to osl_app (RLS still applies)
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA core TO osl_app;
GRANT SELECT ON ALL TABLES IN SCHEMA dw TO osl_app;
