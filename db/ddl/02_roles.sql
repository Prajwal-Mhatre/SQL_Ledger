-- App login used by the API (dev defaults; change in prod)
DO $$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'osl_app') THEN
      CREATE ROLE osl_app LOGIN PASSWORD 'osl_app' NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;
   END IF;
END$$;

GRANT USAGE ON SCHEMA core, dw TO osl_app;

-- Grant on existing tables; default privileges for future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA core GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO osl_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA dw   GRANT SELECT ON TABLES TO osl_app;
