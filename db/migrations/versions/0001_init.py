from __future__ import annotations
from pathlib import Path
from alembic import op

# revision identifiers, used by Alembic.
revision = "0001_init"
down_revision = None
branch_labels = None
depends_on = None

def _run_sql(rel_path: str):
    base = Path(__file__).resolve().parents[2] / "ddl"
    sql_path = base / rel_path
    with open(sql_path, "r", encoding="utf-8") as f:
        op.execute(f.read())

def upgrade() -> None:
    # Order matters
    _run_sql("00_schemas.sql")
    _run_sql("01_extensions.sql")
    _run_sql("02_roles.sql")

    _run_sql("10_tables_core.sql")
    _run_sql("11_constraints_core.sql")
    _run_sql("12_indexes_core.sql")
    _run_sql("13_rls_core.sql")

    _run_sql("20_tables_dw.sql")
    _run_sql("21_dw_scd2.sql")  
    _run_sql("30_mv_current_stock.sql")

    _run_sql("40_partitions_stock_ledger.sql")

def downgrade() -> None:
    # Simpler: drop schemas cascade
    op.execute("DROP SCHEMA IF EXISTS dw CASCADE;")
    op.execute("DROP SCHEMA IF EXISTS core CASCADE;")
    # roles/extensions left for simplicity
