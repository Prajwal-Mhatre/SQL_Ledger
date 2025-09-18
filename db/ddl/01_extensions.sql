-- pgcrypto => gen_random_uuid()
CREATE EXTENSION IF NOT EXISTS pgcrypto;
COMMENT ON EXTENSION pgcrypto IS 'Used for UUID generation, op_id idempotency keys.';

-- btree_gist => for exclusion constraints on ranges + equality on ints/uuids
CREATE EXTENSION IF NOT EXISTS btree_gist;
COMMENT ON EXTENSION btree_gist IS 'Used by exclusion constraint on holds.valid_range.';

-- trigram for fuzzy search (optional but handy)
CREATE EXTENSION IF NOT EXISTS pg_trgm;
COMMENT ON EXTENSION pg_trgm IS 'Optional: trigram search on product names.';

-- track plans for perf comparisons
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
COMMENT ON EXTENSION pg_stat_statements IS 'Collects statement stats for performance analysis.';
