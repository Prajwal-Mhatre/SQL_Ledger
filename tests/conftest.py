from __future__ import annotations

import importlib
import sys
from urllib.parse import urlparse

import pytest

from tests.__init__ import pg_url, engine_app, tenant_ids  # re-export fixtures for pytest discovery


@pytest.fixture()
def api_client(pg_url: str, monkeypatch):
    parsed = urlparse(pg_url)
    db = parsed.path.lstrip("/")
    app_url = f"postgresql+psycopg://osl_app:osl_app@{parsed.hostname}:{parsed.port}/{db}"
    monkeypatch.setenv("DATABASE_URL", app_url)
    monkeypatch.setenv("API_TOKEN", "test-token")

    if "backend.app" in sys.modules:
        module = sys.modules["backend.app"]
        engine = getattr(module, "engine", None)
        if engine is not None:
            engine.dispose()
        sys.modules.pop("backend.app")
    app_module = importlib.import_module("backend.app")
    return app_module.app.test_client(), app_module
