CREATE SCHEMA IF NOT EXISTS core;
COMMENT ON SCHEMA core IS 'OLTP entities (tenants, products, warehouses, lots, orders, holds, stock_ledger).';

CREATE SCHEMA IF NOT EXISTS dw;
COMMENT ON SCHEMA dw IS 'Analytics schema: dimensions, facts, materialized views.';
