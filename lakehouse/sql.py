"""SQL builders for the bronze -> silver CDC merge and the gold models.

Every statement is generated in a dialect shared by Spark SQL (Glue + Iceberg) and
DuckDB, so the exact SQL that runs in production is exercised by the local test suite.
Only DDL differs between engines.
"""

from __future__ import annotations

from typing import Iterable, Optional, Sequence

from .spec import DELETED_COL, FILE_COL, TENANT_COL, TS_COL, TableSpec, check_tenant

OP_COL = "Op"  # DMS writes I / U / D (IncludeOpForFullLoad=true marks full load rows I)
ROW_COL = "_row"  # row position within a file, used as the final tie-breaker

SPARK = "spark"
DUCKDB = "duckdb"


def projection_sql(spec: TableSpec, source_view: str, available_columns: Iterable[str], tenant: str) -> str:
    """Select typed columns from one tenant's raw DMS files.

    Source databases drift: an older tenant may lack a column that newer tenants have.
    Missing columns become typed NULLs so every tenant unions into one schema.
    """
    check_tenant(tenant)
    available = {c.lower(): c for c in available_columns}
    for required in (OP_COL, TS_COL, FILE_COL, ROW_COL):
        if required.lower() not in available:
            raise ValueError(f"source view is missing required column {required!r}")

    exprs = [f"CAST('{tenant}' AS STRING) AS {TENANT_COL}"]
    for col, typ in spec.columns.items():
        src = available.get(col.lower())
        value = src if src is not None else "NULL"
        exprs.append(f"CAST({value} AS {typ}) AS {col}")
    exprs += [
        f"CAST({available[OP_COL.lower()]} AS STRING) AS {OP_COL}",
        f"CAST({available[TS_COL.lower()]} AS TIMESTAMP) AS {TS_COL}",
        f"CAST({available[FILE_COL.lower()]} AS STRING) AS {FILE_COL}",
        f"CAST({available[ROW_COL.lower()]} AS BIGINT) AS {ROW_COL}",
    ]
    return "SELECT " + ", ".join(exprs) + f" FROM {source_view}"


def dedup_sql(spec: TableSpec, source_view: str) -> str:
    """Keep only the latest change per key.

    A CDC batch can hold many changes to one row, but MERGE needs exactly one source
    row per target row. Latest = highest commit timestamp, then latest file, then
    latest position in that file.
    """
    keys = ", ".join(spec.merge_keys)
    cols = ", ".join(_staged_columns(spec))
    return (
        f"SELECT {cols} FROM ("
        f"SELECT *, ROW_NUMBER() OVER (PARTITION BY {keys} "
        f"ORDER BY {TS_COL} DESC, {FILE_COL} DESC, {ROW_COL} DESC) AS _rn "
        f"FROM {source_view}) ranked WHERE _rn = 1"
    )


def merge_sql(
    spec: TableSpec,
    target: str,
    staged_view: str,
    tenants: Optional[Sequence[str]] = None,
) -> str:
    """Upsert the deduplicated changes into the silver table.

    - Deletes are soft (``_is_deleted``) so re-processing an old file can never
      resurrect a deleted row, and downstream models can still see deletion history.
    - ``s._dms_ts >= t._dms_ts`` makes the merge idempotent and safe against
      late or replayed files.
    - Restricting the target to this batch's tenants scopes Iceberg's conflict
      detection, so parallel Glue runs over disjoint tenant batches commit cleanly.
    """
    on = [f"t.{k} = s.{k}" for k in spec.merge_keys]
    if tenants:
        quoted = ", ".join(f"'{check_tenant(t)}'" for t in tenants)
        on.append(f"t.{TENANT_COL} IN ({quoted})")

    is_deleted = f"(s.{OP_COL} = 'D')"
    update_cols = [c for c in spec.silver_columns if c not in spec.merge_keys and c != DELETED_COL]
    updates = [f"{c} = s.{c}" for c in update_cols] + [f"{DELETED_COL} = {is_deleted}"]

    insert_cols = list(spec.silver_columns)
    insert_vals = [is_deleted if c == DELETED_COL else f"s.{c}" for c in insert_cols]

    return (
        f"MERGE INTO {target} AS t USING {staged_view} AS s "
        f"ON {' AND '.join(on)} "
        f"WHEN MATCHED AND s.{TS_COL} >= t.{TS_COL} THEN UPDATE SET {', '.join(updates)} "
        f"WHEN NOT MATCHED THEN INSERT ({', '.join(insert_cols)}) "
        f"VALUES ({', '.join(insert_vals)})"
    )


def create_table_sql(spec: TableSpec, target: str, engine: str) -> str:
    cols = ", ".join(f"{c} {t}" for c, t in spec.silver_columns.items())
    if engine == SPARK:
        return (
            f"CREATE TABLE IF NOT EXISTS {target} ({cols}) USING iceberg "
            f"PARTITIONED BY ({TENANT_COL}) TBLPROPERTIES ("
            "'format-version'='2', "
            "'write.merge.mode'='merge-on-read', "
            "'write.update.mode'='merge-on-read', "
            "'write.delete.mode'='merge-on-read', "
            "'commit.retry.num-retries'='10')"
        )
    if engine == DUCKDB:
        return f"CREATE TABLE IF NOT EXISTS {target} ({cols})"
    raise ValueError(f"unknown engine {engine!r}")


def ctas_sql(target: str, select_sql: str, engine: str) -> str:
    select_sql = select_sql.strip().rstrip(";")
    if engine == SPARK:
        return f"CREATE OR REPLACE TABLE {target} USING iceberg AS {select_sql}"
    if engine == DUCKDB:
        return f"CREATE OR REPLACE TABLE {target} AS {select_sql}"
    raise ValueError(f"unknown engine {engine!r}")


def gold_model_name(stem: str) -> str:
    """``01_tenant_daily_revenue`` -> ``tenant_daily_revenue`` (the prefix only orders builds)."""
    head, _, rest = stem.partition("_")
    return rest if head.isdigit() and rest else stem


def _staged_columns(spec: TableSpec):
    cols = [c for c in spec.silver_columns if c != DELETED_COL]
    return cols + [OP_COL, ROW_COL]
