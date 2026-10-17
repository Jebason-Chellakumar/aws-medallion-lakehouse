"""Run the generated MERGE / dedup SQL against DuckDB to verify CDC semantics."""

from datetime import datetime, timedelta

import duckdb
import pytest

from lakehouse import sql as q
from lakehouse.spec import SpecError, TableSpec, parse_specs

SPEC = TableSpec(
    name="orders",
    source_schema="public",
    primary_key=("order_id",),
    columns={"order_id": "BIGINT", "status": "STRING", "amount": "DECIMAL(12,2)"},
)
T0 = datetime(2026, 10, 1)


@pytest.fixture
def con():
    c = duckdb.connect()
    c.execute("CREATE SCHEMA silver")
    c.execute(q.create_table_sql(SPEC, "silver.orders", q.DUCKDB))
    return c


def apply(
    con, rows, tenant="acme", columns=("Op", "_dms_ts", "_src_file", "_row", "order_id", "status", "amount")
):
    """Load raw change rows, then run projection -> dedup -> merge exactly as the jobs do."""
    con.execute("CREATE OR REPLACE TEMP TABLE raw (" + ", ".join(f"{c} VARCHAR" for c in columns) + ")")
    con.executemany(f"INSERT INTO raw VALUES ({', '.join('?' for _ in columns)})", rows)
    con.execute("CREATE OR REPLACE TEMP VIEW staged_raw AS " + q.projection_sql(SPEC, "raw", columns, tenant))
    con.execute("CREATE OR REPLACE TEMP TABLE staged AS " + q.dedup_sql(SPEC, "staged_raw"))
    con.execute(q.merge_sql(SPEC, "silver.orders", "staged", tenants=[tenant]))


def row(op, seconds, order_id, status, amount="10", file="f1", pos=0):
    return [op, str(T0 + timedelta(seconds=seconds)), file, pos, order_id, status, amount]


def state(con):
    return con.execute(
        "SELECT tenant_id, order_id, status, _is_deleted FROM silver.orders ORDER BY ALL"
    ).fetchall()


def test_latest_change_per_key_wins(con):
    apply(
        con,
        [
            row("I", 0, 1, "pending", pos=0),
            row("U", 2, 1, "delivered", pos=2),
            row("U", 1, 1, "shipped", pos=1),
        ],
    )
    assert state(con) == [("acme", 1, "delivered", False)]


def test_same_timestamp_later_row_in_file_wins(con):
    apply(con, [row("U", 5, 1, "shipped", pos=0), row("U", 5, 1, "refunded", pos=1)])
    assert state(con) == [("acme", 1, "refunded", False)]


def test_same_timestamp_later_file_wins(con):
    apply(con, [row("U", 5, 1, "late", file="f2", pos=0), row("U", 5, 1, "early", file="f1", pos=9)])
    assert state(con) == [("acme", 1, "late", False)]


def test_delete_is_soft_and_survives_replay(con):
    apply(con, [row("I", 0, 1, "pending")])
    apply(con, [row("D", 10, 1, "pending")])
    assert state(con) == [("acme", 1, "pending", True)]
    # Replaying the original insert (older) must not resurrect the row.
    apply(con, [row("I", 0, 1, "pending")])
    assert state(con) == [("acme", 1, "pending", True)]


def test_reinsert_after_delete(con):
    apply(con, [row("I", 0, 1, "pending"), row("D", 1, 1, "pending")])
    apply(con, [row("I", 2, 1, "new")])
    assert state(con) == [("acme", 1, "new", False)]


def test_merge_is_idempotent(con):
    batch = [row("I", 0, 1, "pending"), row("I", 0, 2, "pending"), row("U", 3, 2, "shipped")]
    apply(con, batch)
    first = state(con)
    apply(con, batch)
    assert state(con) == first


def test_older_change_does_not_overwrite_newer(con):
    apply(con, [row("U", 10, 1, "delivered")])
    apply(con, [row("U", 5, 1, "shipped")])
    assert state(con) == [("acme", 1, "delivered", False)]


def test_same_key_in_different_tenants_is_independent(con):
    apply(con, [row("I", 0, 1, "acme-row")], tenant="acme")
    apply(con, [row("I", 0, 1, "globex-row")], tenant="globex")
    apply(con, [row("D", 1, 1, "acme-row")], tenant="acme")
    assert state(con) == [("acme", 1, "acme-row", True), ("globex", 1, "globex-row", False)]


def test_missing_source_column_becomes_typed_null(con):
    cols = ("Op", "_dms_ts", "_src_file", "_row", "order_id", "status")  # no amount
    apply(con, [["I", str(T0), "f", 0, 1, "pending"]], columns=cols)
    assert con.execute("SELECT amount FROM silver.orders").fetchall() == [(None,)]


def test_projection_requires_dms_metadata():
    with pytest.raises(ValueError, match="_dms_ts"):
        q.projection_sql(SPEC, "raw", ["Op", "_src_file", "_row", "order_id"], "acme")


def test_projection_is_case_insensitive_to_source_columns():
    sql = q.projection_sql(SPEC, "raw", ["OP", "_DMS_TS", "_src_file", "_row", "Order_ID"], "acme")
    assert "CAST(Order_ID AS BIGINT) AS order_id" in sql
    assert "CAST(OP AS STRING) AS Op" in sql


def test_spark_ddl_is_partitioned_iceberg_v2():
    ddl = q.create_table_sql(SPEC, "glue_catalog.silver.orders", q.SPARK)
    assert "USING iceberg" in ddl
    assert "PARTITIONED BY (tenant_id)" in ddl
    assert "'format-version'='2'" in ddl


def test_merge_scopes_target_to_batch_tenants():
    sql = q.merge_sql(SPEC, "t", "s", tenants=["a", "b"])
    assert "t.tenant_id IN ('a', 'b')" in sql


@pytest.mark.parametrize("tenant", ["a'; DROP TABLE x; --", "", "a b"])
def test_rejects_unsafe_tenant_ids(tenant):
    with pytest.raises(SpecError):
        q.merge_sql(SPEC, "t", "s", tenants=[tenant])


@pytest.mark.parametrize(
    "body, message",
    [
        ({"source_schema": "public", "primary_key": [], "columns": {"a": "BIGINT"}}, "primary_key"),
        ({"source_schema": "public", "primary_key": ["b"], "columns": {"a": "BIGINT"}}, "not in columns"),
        ({"source_schema": "public", "primary_key": ["a"], "columns": {"a": "BLOB"}}, "unsupported type"),
        (
            {
                "source_schema": "public",
                "primary_key": ["a"],
                "columns": {"a": "BIGINT", "tenant_id": "STRING"},
            },
            "reserved",
        ),
        (
            {"source_schema": "public", "primary_key": ["a"], "columns": {"a; drop": "BIGINT"}},
            "invalid identifier",
        ),
    ],
)
def test_spec_validation(body, message):
    with pytest.raises(SpecError, match=message):
        parse_specs({"t": body})
