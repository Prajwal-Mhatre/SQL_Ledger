from __future__ import annotations
from sqlalchemy import text
from sqlalchemy.engine import Connection

def refresh_current_stock_mv(conn: Connection) -> None:
    """
    Full MV refresh for dw.current_stock_mv.

    Why full refresh?
    - It's deterministic and simple for a small/medium ledger.
    - Incremental refresh requires additional bookkeeping (e.g., change tables).
    - We keep a unique index, so a future `REFRESH MATERIALIZED VIEW CONCURRENTLY`
      is possible if we want to avoid blocking readers.
    """
    conn.execute(text("REFRESH MATERIALIZED VIEW dw.current_stock_mv"))
