-- name: select_order_lines
SELECT ol.id AS order_line_id, ol.product_id, ol.qty
FROM core.order_lines ol
JOIN core.orders o ON o.id = ol.order_id
WHERE ol.order_id = :order_id
  AND o.status IN ('open','allocated');

-- name: allocation_candidates
/*
Deterministic candidate selection with row locking:
1) Lock lots for this product in a stable order (expiry->lot_id).
2) Compute on-hand (ledger sum) and subtract active holds.
3) Return rows with available_qty > 0 in a deterministic lock order:
   (warehouse_id → lot_id → location_id → expiry_date)
This ensures every worker acquires row locks in the same sequence,
minimizing deadlocks.
*/
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
  GREATEST(0, s.onhand - COALESCE(h.reserved,0)) AS available_qty,
  cl.expiry_date
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
ORDER BY s.warehouse_id, cl.lot_id, s.location_id, cl.expiry_date NULLS LAST
LIMIT :take_limit;

-- name: insert_hold
INSERT INTO core.holds
  (id, tenant_id, order_id, order_line_id, product_id, lot_id, warehouse_id, location_id, qty)
VALUES
  (:id, current_setting('app.tenant_id')::uuid, :order_id, :order_line_id,
   :product_id, :lot_id, :warehouse_id, :location_id, :qty);

-- name: insert_ledger_reserve
INSERT INTO core.stock_ledger
  (tenant_id, ts, event_type, warehouse_id, location_id, product_id, lot_id,
   order_id, order_line_id, qty_delta, reason, op_id)
VALUES
  (current_setting('app.tenant_id')::uuid, now(), 'RESERVE',
   :wh, :loc, :prod, :lot, :ord, :ol, :delta, :reason, :op_id);

-- name: mark_order_allocated
UPDATE core.orders
   SET status = 'allocated'
 WHERE id = :order_id
   AND tenant_id = current_setting('app.tenant_id')::uuid
   AND status = 'open';
