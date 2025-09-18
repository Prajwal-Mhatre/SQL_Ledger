from __future__ import annotations
import os
import uuid
from pathlib import Path
from typing import Any, Dict

from flask import Flask, request, jsonify, g, send_from_directory
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, Connection

from backend.services.allocation import allocate_order
from backend.services.refresh_materialized import refresh_current_stock_mv

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg://osl_app:osl_app@localhost:5432/osl")
DEFAULT_TENANT_ID = os.getenv("DEFAULT_TENANT_ID", "00000000-0000-0000-0000-000000000001")
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN")  # optional; enables /admin routes when set


app = Flask(__name__, static_folder=None)
engine: Engine = create_engine(DATABASE_URL, future=True, pool_pre_ping=True)

# Load SQL files
SQL_DIR = Path(__file__).resolve().parents[1] / "db" / "queries"
with open(SQL_DIR / "product_search.sql", "r", encoding="utf-8") as f:
    SQL_PRODUCT_SEARCH = text(f.read())
with open(SQL_DIR / "current_stock.sql", "r", encoding="utf-8") as f:
    SQL_CURRENT_STOCK = text(f.read())

def require_tenant() -> uuid.UUID:
    tid = request.headers.get("X-Tenant-Id", DEFAULT_TENANT_ID)
    try:
        return uuid.UUID(tid)
    except Exception:
        raise ValueError("Invalid or missing X-Tenant-Id header")

@app.before_request
def open_db_conn():
    g.db = engine.connect()  # type: Connection
    g.db.execute(text("SET application_name = 'open-stock-ledger'"))

@app.after_request
def close_db_conn(response):
    conn: Connection | None = getattr(g, "db", None)
    if conn is not None:
        conn.close()
    return response

def set_tenant(conn: Connection, tenant_id: uuid.UUID):
    conn.execute(text("SET app.tenant_id = :tid"), {"tid": str(tenant_id)})

@app.get("/health")
def health():
    return jsonify({"ok": True})

@app.get("/")
def index_html():
    root = Path(__file__).resolve().parents[1] / "frontend"
    return send_from_directory(root, "index.html")

@app.get("/app.js")
def app_js():
    root = Path(__file__).resolve().parents[1] / "frontend"
    return send_from_directory(root, "app.js")

@app.get("/styles.css")
def styles_css():
    root = Path(__file__).resolve().parents[1] / "frontend"
    return send_from_directory(root, "styles.css")

@app.get("/api/products")
def product_search():
    tenant_id = require_tenant()
    q = request.args.get("q", "").strip()
    like = f"%{q}%"
    limit = int(request.args.get("limit", "20"))
    offset = int(request.args.get("offset", "0"))

    with g.db.begin():
        set_tenant(g.db, tenant_id)
        rows = g.db.execute(SQL_PRODUCT_SEARCH, {"q": q, "like": like, "limit": limit, "offset": offset}).mappings().all()
    return jsonify({"items": list(rows)})

@app.post("/api/orders")
def create_order():
    tenant_id = require_tenant()
    payload: Dict[str, Any] = request.get_json(force=True, silent=False) or {}
    external_ref = (payload.get("external_ref") or "").strip()
    lines = payload.get("lines") or []
    if not lines:
        return jsonify({"error": "lines required"}), 400

    with g.db.begin():
        set_tenant(g.db, tenant_id)
        order_id = uuid.uuid4()
        g.db.execute(text("""
            INSERT INTO core.orders (id, tenant_id, external_ref, status) 
            VALUES (:id, current_setting('app.tenant_id')::uuid, :ref, 'open')
        """), {"id": str(order_id), "ref": external_ref})
        for line in lines:
            product_id = line["product_id"]
            qty = int(line["qty"])
            if qty <= 0:
                return jsonify({"error": "qty must be > 0"}), 400
            g.db.execute(text("""
                INSERT INTO core.order_lines (tenant_id, order_id, product_id, qty)
                VALUES (current_setting('app.tenant_id')::uuid, :order_id, :product_id, :qty)
            """), {"order_id": str(order_id), "product_id": product_id, "qty": qty})
    return jsonify({"order_id": str(order_id)}), 201

@app.post("/api/orders/<order_id>/allocate")
def allocate(order_id: str):
    tenant_id = require_tenant()
    payload = request.get_json(force=True, silent=True) or {}
    try:
        res = allocate_order(engine, tenant_id=uuid.UUID(str(tenant_id)), order_id=uuid.UUID(order_id), request_hint=payload)
        return jsonify(res)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.get("/api/current_stock")
def current_stock():
    tenant_id = require_tenant()
    product_id = request.args.get("product_id")
    if not product_id:
        return jsonify({"error": "product_id required"}), 400
    with g.db.begin():
        set_tenant(g.db, tenant_id)
        rows = g.db.execute(SQL_CURRENT_STOCK, {"product_id": product_id}).mappings().all()
    return jsonify({"items": list(rows)})

@app.post("/api/refresh_current_stock")
def refresh_mv():
    tenant_id = require_tenant()
    # tenant_id not strictly needed for MV refresh; RLS doesn’t apply to MV creation
    with g.db.begin():
        set_tenant(g.db, tenant_id)
        refresh_current_stock_mv(g.db)
    return jsonify({"refreshed": True})


@app.post("/admin/refresh_mv")
def admin_refresh_mv():
    """
    Manual MV refresh endpoint behind a simple env guard.
    - Enable by setting ADMIN_TOKEN in the environment.
    - Callers must include header: X-Admin-Token: <ADMIN_TOKEN>
    """
    if not ADMIN_TOKEN:
        return ("Not Found", 404)
    if request.headers.get("X-Admin-Token") != ADMIN_TOKEN:
        return ("Unauthorized", 401)

    # No tenant required; MV holds aggregated rows for all tenants.
    with g.db.begin():
        refresh_current_stock_mv(g.db)
    return jsonify({"refreshed": True})


if __name__ == "__main__":
    app.run("0.0.0.0", 8000, debug=True)
