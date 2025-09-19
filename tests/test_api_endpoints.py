from __future__ import annotations
import uuid

from sqlalchemy import text


def _insert_core_refs(conn, tenant_id, product_id, warehouse_id, location_id, lot_id, sku="SKU-API"):
    conn.execute(text("SET app.tenant_id = :t"), {"t": str(tenant_id)})
    conn.execute(
        text(
            """
            INSERT INTO core.products (id, tenant_id, sku, name, price_cents)
            VALUES (:id, current_setting('app.tenant_id')::uuid, :sku, :name, :price)
            ON CONFLICT (id) DO NOTHING
            """
        ),
        {"id": str(product_id), "sku": sku, "name": "API Product", "price": 1250},
    )
    conn.execute(
        text(
            """
            INSERT INTO core.warehouses (id, tenant_id, code, name)
            VALUES (:id, current_setting('app.tenant_id')::uuid, 'WH-API', 'Warehouse API')
            ON CONFLICT (id) DO NOTHING
            """
        ),
        {"id": str(warehouse_id)},
    )
    conn.execute(
        text(
            """
            INSERT INTO core.locations (id, tenant_id, warehouse_id, code, name)
            VALUES (:id, current_setting('app.tenant_id')::uuid, :wh, 'LOC-API', 'Location API')
            ON CONFLICT (id) DO NOTHING
            """
        ),
        {"id": str(location_id), "wh": str(warehouse_id)},
    )
    conn.execute(
        text(
            """
            INSERT INTO core.lots (id, tenant_id, product_id, lot_number)
            VALUES (:id, current_setting('app.tenant_id')::uuid, :prod, 'LOT-API')
            ON CONFLICT (id) DO NOTHING
            """
        ),
        {"id": str(lot_id), "prod": str(product_id)},
    )


def test_inventory_event_endpoint_refreshes_mv(api_client, engine_app, tenant_ids):
    client, _ = api_client
    token = "test-token"
    tenant_id, _ = tenant_ids
    product_id = uuid.uuid4()
    warehouse_id = uuid.uuid4()
    location_id = uuid.uuid4()
    lot_id = uuid.uuid4()

    with engine_app.begin() as conn:
        _insert_core_refs(conn, tenant_id, product_id, warehouse_id, location_id, lot_id)

    headers = {"X-Tenant-Id": str(tenant_id), "X-Api-Token": token}
    resp = client.post(
        "/api/stock_events",
        json={
            "event_type": "RECEIPT",
            "warehouse_id": str(warehouse_id),
            "location_id": str(location_id),
            "product_id": str(product_id),
            "lot_id": str(lot_id),
            "qty": 10,
        },
        headers=headers,
    )
    assert resp.status_code == 201

    with engine_app.begin() as conn:
        conn.execute(text("SET app.tenant_id = :t"), {"t": str(tenant_id)})
        qty = conn.execute(
            text(
                """
                SELECT qty FROM dw.current_stock_mv
                 WHERE tenant_id = current_setting('app.tenant_id')::uuid
                   AND product_id = :prod AND warehouse_id = :wh AND location_id = :loc AND lot_id = :lot
                """
            ),
            {"prod": str(product_id), "wh": str(warehouse_id), "loc": str(location_id), "lot": str(lot_id)},
        ).scalar_one()
        assert qty == 10

    resp = client.post(
        "/api/stock_events",
        json={
            "event_type": "SHIP",
            "warehouse_id": str(warehouse_id),
            "location_id": str(location_id),
            "product_id": str(product_id),
            "lot_id": str(lot_id),
            "qty": 4,
        },
        headers=headers,
    )
    assert resp.status_code == 201

    with engine_app.begin() as conn:
        conn.execute(text("SET app.tenant_id = :t"), {"t": str(tenant_id)})
        qty = conn.execute(
            text(
                """
                SELECT qty FROM dw.current_stock_mv
                 WHERE tenant_id = current_setting('app.tenant_id')::uuid
                   AND product_id = :prod AND warehouse_id = :wh AND location_id = :loc AND lot_id = :lot
                """
            ),
            {"prod": str(product_id), "wh": str(warehouse_id), "loc": str(location_id), "lot": str(lot_id)},
        ).scalar_one()
        assert qty == 6


def test_product_api_triggers_scd(api_client, engine_app, tenant_ids):
    client, _ = api_client
    token = "test-token"
    tenant_id, _ = tenant_ids

    headers = {"X-Tenant-Id": str(tenant_id), "X-Api-Token": token}
    resp = client.post(
        "/api/products",
        json={
            "sku": "SKU-TEST",
            "name": "Widget",
            "attributes": {"color": "red"},
            "price": "10.50",
        },
        headers=headers,
    )
    assert resp.status_code == 201
    product_id = resp.get_json()["id"]

    resp = client.put(
        f"/api/products/{product_id}",
        json={"name": "Widget Prime", "price": "12.25"},
        headers=headers,
    )
    assert resp.status_code == 200

    with engine_app.begin() as conn:
        conn.execute(text("SET app.tenant_id = :t"), {"t": str(tenant_id)})
        rows = conn.execute(
            text(
                """
                SELECT name, price_cents, is_current, valid_from, valid_to
                  FROM dw.dim_product
                 WHERE tenant_id = current_setting('app.tenant_id')::uuid
                   AND product_nk = :pid
                 ORDER BY valid_from
                """
            ),
            {"pid": product_id},
        ).all()
    assert len(rows) == 2
    first, second = rows
    assert first[0] == "Widget"
    assert first[1] == 1050
    assert first[2] is False
    assert first[4] is not None
    assert second[0] == "Widget Prime"
    assert second[1] == 1225
    assert second[2] is True
    assert second[4] is None


def test_customer_api_updates_dim(api_client, engine_app, tenant_ids):
    client, _ = api_client
    token = "test-token"
    tenant_id, _ = tenant_ids
    headers = {"X-Tenant-Id": str(tenant_id), "X-Api-Token": token}

    resp = client.post(
        "/api/customers",
        json={"code": "CUST-1", "name": "Blue Crate"},
        headers=headers,
    )
    assert resp.status_code == 201
    customer_id = resp.get_json()["id"]

    resp = client.put(
        f"/api/customers/{customer_id}",
        json={"name": "Blue Crate Intl"},
        headers=headers,
    )
    assert resp.status_code == 200

    with engine_app.begin() as conn:
        conn.execute(text("SET app.tenant_id = :t"), {"t": str(tenant_id)})
        rows = conn.execute(
            text(
                """
                SELECT name, is_current
                  FROM dw.dim_customer
                 WHERE tenant_id = current_setting('app.tenant_id')::uuid
                   AND customer_nk = :cid
                 ORDER BY valid_from
                """
            ),
            {"cid": customer_id},
        ).all()
    assert len(rows) == 2
    assert rows[0][0] == "Blue Crate" and rows[0][1] is False
    assert rows[1][0] == "Blue Crate Intl" and rows[1][1] is True
