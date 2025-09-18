-- name: upsert_dim_product
WITH cur AS (
  SELECT product_sk, sku, name, attrs
  FROM dw.dim_product
  WHERE tenant_id = :tenant_id AND product_nk = :product_nk AND is_current
  FOR UPDATE
), change AS (
  SELECT
    (SELECT sku FROM cur)  IS DISTINCT FROM :sku   AS sku_changed,
    (SELECT name FROM cur) IS DISTINCT FROM :name  AS name_changed,
    (SELECT attrs FROM cur) IS DISTINCT FROM :attrs AS attrs_changed
), do_close AS (
  UPDATE dw.dim_product
     SET is_current = false, valid_to = now()
   WHERE (SELECT coalesce(sku_changed, true) OR coalesce(name_changed, true) OR coalesce(attrs_changed, true) FROM change)
     AND tenant_id = :tenant_id AND product_nk = :product_nk AND is_current
  RETURNING 1
)
INSERT INTO dw.dim_product (tenant_id, product_nk, sku, name, attrs, valid_from, valid_to, is_current)
SELECT :tenant_id, :product_nk, :sku, :name, :attrs, now(), NULL, true
WHERE NOT EXISTS (SELECT 1 FROM cur)
   OR (SELECT coalesce(sku_changed, true) OR coalesce(name_changed, true) OR coalesce(attrs_changed, true) FROM change)
RETURNING product_sk;

-- name: upsert_dim_customer
WITH cur AS (
  SELECT customer_sk, name
  FROM dw.dim_customer
  WHERE tenant_id = :tenant_id AND customer_nk = :customer_nk AND is_current
  FOR UPDATE
), change AS (
  SELECT (SELECT name FROM cur) IS DISTINCT FROM :name AS changed
), do_close AS (
  UPDATE dw.dim_customer
     SET is_current = false, valid_to = now()
   WHERE (SELECT coalesce(changed, true) FROM change)
     AND tenant_id = :tenant_id AND customer_nk = :customer_nk AND is_current
  RETURNING 1
)
INSERT INTO dw.dim_customer (tenant_id, customer_nk, name, valid_from, valid_to, is_current)
SELECT :tenant_id, :customer_nk, :name, now(), NULL, true
WHERE NOT EXISTS (SELECT 1 FROM cur) OR (SELECT coalesce(changed, true) FROM change)
RETURNING customer_sk;

-- name: upsert_dim_warehouse
WITH cur AS (
  SELECT warehouse_sk, code, name
  FROM dw.dim_warehouse
  WHERE tenant_id = :tenant_id AND warehouse_nk = :warehouse_nk AND is_current
  FOR UPDATE
), change AS (
  SELECT
    (SELECT code FROM cur) IS DISTINCT FROM :code AS code_changed,
    (SELECT name FROM cur) IS DISTINCT FROM :name AS name_changed
), do_close AS (
  UPDATE dw.dim_warehouse
     SET is_current = false, valid_to = now()
   WHERE (SELECT coalesce(code_changed, true) OR coalesce(name_changed, true) FROM change)
     AND tenant_id = :tenant_id AND warehouse_nk = :warehouse_nk AND is_current
  RETURNING 1
)
INSERT INTO dw.dim_warehouse (tenant_id, warehouse_nk, code, name, valid_from, valid_to, is_current)
SELECT :tenant_id, :warehouse_nk, :code, :name, now(), NULL, true
WHERE NOT EXISTS (SELECT 1 FROM cur)
   OR (SELECT coalesce(code_changed, true) OR coalesce(name_changed, true) FROM change)
RETURNING warehouse_sk;
