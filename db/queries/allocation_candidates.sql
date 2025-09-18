-- name: allocation_candidates
-- params: product_id(uuid), take_limit(int)
WITH candidate_lots AS (
  SELECT l.id AS lot_id, l.expiry_date
  FROM core.lots l
  WHERE l.tenant_id = current_setting('app.tenant_id', true)::uuid
    AND l.product_id = :product_id
    AND l.is_active
  ORDER BY l.expiry_date NULLS LAST, l.id
  FOR UPDATE OF l SKIP LOCKED
)
SELECT
  cl.lot_id,
  s.warehouse_id,
  s.location_id,
  GREATEST(0, s.onhand - COALESCE(h.reserved,0)) AS available_qty
FROM candidate_lots cl
CROSS JOIN LATERAL (
  SELECT
    sl.warehouse_id,
    sl.location_id,
    COALESCE(SUM(sl.qty_delta),0) AS onhand
  FROM core.stock_ledger sl
  WHERE sl.tenant_id = current_setting('app.tenant_id', true)::uuid
    AND sl.product_id = :product_id
    AND sl.lot_id = cl.lot_id
  GROUP BY sl.warehouse_id, sl.location_id
) s
LEFT JOIN LATERAL (
  SELECT
    h.warehouse_id,
    h.location_id,
    COALESCE(SUM(h.qty),0) AS reserved
  FROM core.holds h
  WHERE h.tenant_id = current_setting('app.tenant_id', true)::uuid
    AND h.product_id = :product_id
    AND h.lot_id = cl.lot_id
    AND h.released_at IS NULL
  GROUP BY h.warehouse_id, h.location_id
) h ON h.warehouse_id = s.warehouse_id AND h.location_id = s.location_id
WHERE GREATEST(0, s.onhand - COALESCE(h.reserved,0)) > 0
ORDER BY cl.expiry_date NULLS LAST, cl.lot_id, s.warehouse_id, s.location_id
LIMIT :take_limit;
