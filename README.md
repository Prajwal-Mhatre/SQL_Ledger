# Open Stock Ledger — Multi‑tenant Inventory & Warehouse Analytics on PostgreSQL

A small but production‑style inventory system that keeps an **auditable stock ledger** and serves both operational queries (current stock, allocations) and analytics (ABC, aging, history) from a single PostgreSQL instance. It demonstrates advanced SQL (RLS, functional/partial indexes, exclusion constraints, partitioning, materialized views, SCD2 later, and deadlock handling) without pretending to be web‑scale.

## Why?

- **Auditable truth:** Real inventory must be reconstructible. A ledger of immutable events (RECEIPT/SHIP/ADJUST/RESERVE/RELEASE) is the simplest, most reliable source of truth.
- **Correctness under concurrency:** Allocation is inherently multi‑worker. We handle locks, deadlocks, retries, and idempotency to avoid double‑booking.
- **Tenant isolation:** Row‑Level Security with a tenant context enforces isolation at the database layer.
- **Operational + analytical together:** Partitioning + materialized views + (later) SCD2 let Postgres serve both cleanly.
- **Signal > scope:** Every feature exists to support correctness, performance, and clarity.

## How it works (brief)
- **Core entities:** tenants, users, products (JSONB attrs, FTS), warehouses/locations, lots/expiry, orders & lines, holds, and the **stock_ledger** (event‑sourced with `qty_delta` and idempotency `op_id`).
- **Security:** The app sets `app.tenant_id`; RLS policies restrict every query to that tenant automatically.
- **Performance:** Functional/partial indexes, JSONB GIN + FTS. **Ledger partitioned monthly** + **BRIN**. **Materialized view** `dw.current_stock_mv` for snappy reads.
- **Concurrency:** Allocation uses consistent ordering, `FOR UPDATE SKIP LOCKED` on lots, short timeouts, **advisory locks per order**, and automatic retries on 40P01/40001.

## Flow
1. **Stock arrives:** Insert a RECEIPT event (+qty).
2. **Allocate:** Workers select candidates via LATERAL with `FOR UPDATE SKIP LOCKED`, insert a HOLD and a `RESERVE` event (−qty).
3. **Ship:** A SHIP event (−qty). MV reflects net stock after refresh.

## Run it

```bash
# 1) Bring up Postgres
cp .env.example .env
docker compose up -d postgres

# 2) Migrate (creates schemas, tables, RLS, MV, partitions, roles)
alembic -c alembic.ini upgrade head

# 3) Bring up API + Adminer
docker compose up -d

# UI: http://localhost:8000   DB UI: http://localhost:8080
