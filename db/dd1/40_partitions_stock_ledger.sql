SET search_path = core, public;

-- Parent already created in 10_tables_core.sql.
-- Create a helper to ensure monthly partition exists.
CREATE OR REPLACE FUNCTION core.ensure_stock_ledger_partition(p_month_start date)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  part_start timestamptz := date_trunc('month', p_month_start)::timestamptz;
  part_end   timestamptz := (date_trunc('month', p_month_start) + INTERVAL '1 month')::timestamptz;
  part_name  text := format('stock_ledger_%s', to_char(part_start, 'YYYY_MM'));
  full_name  text := format('core.%I', part_name);
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                 WHERE n.nspname='core' AND c.relname=part_name) THEN
    EXECUTE format(
      'CREATE TABLE %s PARTITION OF core.stock_ledger
         FOR VALUES FROM (%L) TO (%L);',
      full_name, part_start, part_end
    );
    -- BRIN per partition for ts
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I_brin_ts ON %s USING BRIN (ts) WITH (pages_per_range=128);',
                   part_name, full_name);
  END IF;
END$$;

-- Create partitions for current and next 2 months (demo-friendly)
DO $$
DECLARE
  m date := date_trunc('month', now())::date;
  i int;
BEGIN
  FOR i IN 0..2 LOOP
    PERFORM core.ensure_stock_ledger_partition(m + (i||' months')::interval);
  END LOOP;
END$$;

COMMENT ON FUNCTION core.ensure_stock_ledger_partition(date)
  IS 'Creates a monthly time-range partition for stock_ledger and a BRIN index on ts.';
