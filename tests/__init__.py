from __future__ import annotations
import os
import uuid
import time
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from alembic.config import Config
from alembic import command
from testcontainers.postgres import PostgresContainer

@pytest.fixture(scope="session")
def pg_url() -> str:
    with PostgresContainer("postgres:16") as pg:
        url = pg.get_connection_url().replace("postgresql://", "postgresql+psycopg://")
        # Create app role inside the DB before alembic reads roles.sql? We run full migration which creates the role.
        os.environ["DATABASE_URL"] = url
        # Run migrations
        cfg = Config("alembic.ini")
        cfg.set_main_option("sqlalchemy.url", url)
        command.upgrade(cfg, "head")
        yield url

@pytest.fixture()
def engine_app(pg_url: str) -> Engine:
    # Connect as app role (created by migration): osl_app/osl_app
    # Replace host/port/db from pg_url, but specify user/pass
    # pg_url is like postgresql+psycopg://test:test@0.0.0.0:NNNN/test
    # Build an osl_app URL on same host/port/db
    from urllib.parse import urlparse
    p = urlparse(pg_url)
    db = p.path.lstrip("/")
    app_url = f"postgresql+psycopg://osl_app:osl_app@{p.hostname}:{p.port}/{db}"
    eng = create_engine(app_url, future=True, pool_pre_ping=True)
    # Wait for connectivity
    for _ in range(20):
        try:
            with eng.connect() as c:
                c.execute(text("SELECT 1"))
            break
        except Exception:
            time.sleep(0.2)
    return eng

@pytest.fixture()
def tenant_ids(engine_app: Engine):
    t1 = uuid.uuid4()
    t2 = uuid.uuid4()
    with engine_app.begin() as conn:
        conn.execute(text("SET app.tenant_id = :t"), {"t": str(t1)})
        conn.execute(text("INSERT INTO core.tenants (id, name) VALUES (current_setting('app.tenant_id')::uuid, 'Tenant A')"))
        conn.execute(text("SET app.tenant_id = :t"), {"t": str(t2)})
        conn.execute(text("INSERT INTO core.tenants (id, name) VALUES (current_setting('app.tenant_id')::uuid, 'Tenant B')"))
    return (t1, t2)
