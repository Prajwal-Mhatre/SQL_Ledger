-- name: current_stock
-- params: product_id(uuid)
SELECT
  product_id, warehouse_id, location_id, lot_id, qty
FROM dw.current_stock_mv
WHERE tenant_id = current_setting('app.tenant_id', true)::uuid
  AND product_id = :product_id
ORDER BY warehouse_id, location_id, lot_id;
