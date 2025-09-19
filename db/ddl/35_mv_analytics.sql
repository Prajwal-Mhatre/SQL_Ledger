SET search_path = dw, public;

-- ABC classification materialized view
CREATE MATERIALIZED VIEW IF NOT EXISTS product_abc_mv AS
WITH shipped AS (
  SELECT
    tenant_id,
    product_id,
    SUM(CASE WHEN event_type = 'SHIP' THEN -qty_delta ELSE 0 END)::numeric AS shipped_qty
  FROM core.stock_ledger
  GROUP BY tenant_id, product_id
), ordered AS (
  SELECT
    tenant_id,
    product_id,
    COALESCE(shipped_qty, 0) AS shipped_qty,
    SUM(COALESCE(shipped_qty, 0)) OVER (PARTITION BY tenant_id) AS total_shipped,
    SUM(COALESCE(shipped_qty, 0)) OVER (
      PARTITION BY tenant_id
      ORDER BY COALESCE(shipped_qty, 0) DESC, product_id
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_shipped
  FROM shipped
), classified AS (
  SELECT
    tenant_id,
    product_id,
    shipped_qty,
    total_shipped,
    running_shipped,
    CASE
      WHEN total_shipped <= 0 THEN 'C'
      WHEN running_shipped / total_shipped <= 0.7 THEN 'A'
      WHEN running_shipped / total_shipped <= 0.9 THEN 'B'
      ELSE 'C'
    END AS abc_class,
    COALESCE(running_shipped / NULLIF(total_shipped, 0), 0) AS cumulative_ratio
  FROM ordered
)
SELECT tenant_id, product_id, shipped_qty, total_shipped, cumulative_ratio, abc_class
FROM classified;

CREATE UNIQUE INDEX IF NOT EXISTS ux_product_abc_mv
  ON product_abc_mv (tenant_id, product_id);

COMMENT ON MATERIALIZED VIEW product_abc_mv
  IS 'ABC classification by shipped quantity per tenant (A=top 70%, B=next 20%, C=rest).';

-- Inventory aging materialized view
CREATE MATERIALIZED VIEW IF NOT EXISTS inventory_aging_mv AS
WITH lot_balances AS (
  SELECT
    tenant_id,
    product_id,
    warehouse_id,
    location_id,
    lot_id,
    SUM(qty_delta)::bigint AS qty
  FROM core.stock_ledger
  GROUP BY tenant_id, product_id, warehouse_id, location_id, lot_id
), first_receipts AS (
  SELECT
    tenant_id,
    product_id,
    lot_id,
    MIN(ts) FILTER (WHERE event_type IN ('RECEIPT','ADJUST_IN')) AS first_in
  FROM core.stock_ledger
  GROUP BY tenant_id, product_id, lot_id
), combined AS (
  SELECT
    lb.tenant_id,
    lb.product_id,
    lb.warehouse_id,
    lb.location_id,
    lb.lot_id,
    lb.qty,
    fr.first_in,
    CASE
      WHEN fr.first_in IS NULL THEN NULL
      ELSE DATE_PART('day', now() - fr.first_in)::int
    END AS age_days
  FROM lot_balances lb
  LEFT JOIN first_receipts fr
    ON fr.tenant_id = lb.tenant_id
   AND fr.product_id = lb.product_id
   AND (fr.lot_id IS NOT DISTINCT FROM lb.lot_id)
  WHERE lb.qty > 0
)
SELECT
  tenant_id,
  product_id,
  warehouse_id,
  location_id,
  lot_id,
  qty,
  age_days,
  CASE
    WHEN age_days IS NULL THEN 'unknown'
    WHEN age_days < 30 THEN '0-29'
    WHEN age_days < 60 THEN '30-59'
    WHEN age_days < 90 THEN '60-89'
    ELSE '90+'
  END AS age_bucket
FROM combined;

CREATE INDEX IF NOT EXISTS ix_inventory_aging_mv_lookup
  ON inventory_aging_mv (tenant_id, product_id, warehouse_id, location_id, COALESCE(lot_id, '00000000-0000-0000-0000-000000000000'::uuid));

COMMENT ON MATERIALIZED VIEW inventory_aging_mv
  IS 'On-hand inventory aged by first receipt timestamp with coarse buckets.';

-- Reorder candidates materialized view
CREATE MATERIALIZED VIEW IF NOT EXISTS reorder_candidates_mv AS
WITH current_qty AS (
  SELECT tenant_id, product_id, SUM(qty)::bigint AS on_hand
  FROM dw.current_stock_mv
  GROUP BY tenant_id, product_id
), ship_window AS (
  SELECT
    tenant_id,
    product_id,
    SUM(CASE WHEN event_type = 'SHIP' THEN -qty_delta ELSE 0 END)::numeric AS shipped_30
  FROM core.stock_ledger
  WHERE ts >= now() - INTERVAL '30 days'
  GROUP BY tenant_id, product_id
), base AS (
  SELECT
    COALESCE(c.tenant_id, s.tenant_id) AS tenant_id,
    COALESCE(c.product_id, s.product_id) AS product_id,
    COALESCE(c.on_hand, 0) AS on_hand,
    COALESCE(s.shipped_30, 0) AS shipped_30
  FROM current_qty c
  FULL OUTER JOIN ship_window s
    ON s.tenant_id = c.tenant_id AND s.product_id = c.product_id
)
SELECT
  tenant_id,
  product_id,
  on_hand,
  shipped_30,
  (COALESCE(shipped_30, 0) / 30.0)::numeric AS avg_daily_ship,
  CEIL((COALESCE(shipped_30, 0) / 30.0) * 7)::bigint AS reorder_point,
  (on_hand < CEIL((COALESCE(shipped_30, 0) / 30.0) * 7)) AS needs_reorder
FROM base;

CREATE UNIQUE INDEX IF NOT EXISTS ux_reorder_candidates_mv
  ON reorder_candidates_mv (tenant_id, product_id);

COMMENT ON MATERIALIZED VIEW reorder_candidates_mv
  IS 'Simple reorder heuristic: current on-hand vs 7-day buffer of average daily shipments (30-day lookback).';
