SET search_path = public;

-- Derived "current stock" from ledger (net of all qty_delta).
-- Rationale: Operational screen needs snappy reads; MV avoids resumming ledger on every request.
CREATE MATERIALIZED VIEW IF NOT EXISTS dw.current_stock_mv AS
SELECT
  tenant_id,
  product_id,
  warehouse_id,
  location_id,
  lot_id,
  SUM(qty_delta)::bigint AS qty
FROM core.stock_ledger
GROUP BY tenant_id, product_id, warehouse_id, location_id, lot_id;

-- Index to make lookups fast (and allow CONCURRENT refresh later if needed)
CREATE UNIQUE INDEX IF NOT EXISTS uk_current_stock_key
  ON dw.current_stock_mv (tenant_id, product_id, warehouse_id, location_id, lot_id);

COMMENT ON MATERIALIZED VIEW dw.current_stock_mv
  IS 'Materialized current stock (sum of ledger); refresh on demand.';
