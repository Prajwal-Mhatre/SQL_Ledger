-- name: product_search
-- params: q(text), like(text), limit(int), offset(int)
SELECT
  p.id, p.sku, p.name, p.description
FROM core.products AS p
WHERE p.is_active
  AND (
    p.search_tsv @@ plainto_tsquery('english', :q)
    OR lower(p.sku) LIKE lower(:like)
  )
ORDER BY ts_rank(p.search_tsv, plainto_tsquery('english', :q)) DESC, p.id
LIMIT :limit OFFSET :offset;
