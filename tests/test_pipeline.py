"""End-to-end: DMS-style sample files -> silver -> gold on DuckDB."""

from pathlib import Path

import pytest

from local_lake import pipeline, sample_data

ROOT = Path(__file__).resolve().parent.parent
CONF, GOLD = ROOT / "conf" / "tables.json", ROOT / "gold"


@pytest.fixture
def lake(tmp_path):
    sample_data.generate(tmp_path)
    db = str(tmp_path / "lake.duckdb")
    con = pipeline.run(tmp_path, CONF, GOLD, db)
    return tmp_path, db, con


def one(con, sql):
    return con.execute(sql).fetchone()[0]


def test_silver_reflects_latest_state(lake):
    _, _, con = lake
    status = dict(
        con.execute("SELECT order_id, status FROM silver.orders WHERE tenant_id = 'acme'").fetchall()
    )
    assert status[3] == "delivered"  # two updates in one batch
    assert status[4] == "refunded"  # same commit timestamp, later row wins
    assert status[7] == "cancelled"
    assert one(con, "SELECT amount FROM silver.orders WHERE tenant_id='acme' AND order_id=9") == 95
    assert one(con, "SELECT _is_deleted FROM silver.customers WHERE tenant_id='acme' AND customer_id=5")


def test_every_tenant_and_key_appears_once(lake):
    _, _, con = lake
    assert one(con, "SELECT COUNT(*) FROM silver.orders") == 3 * 10
    assert one(con, "SELECT COUNT(DISTINCT (tenant_id, order_id)) FROM silver.orders") == 30


def test_schema_drift_legacy_tenant(lake):
    _, _, con = lake
    rows = dict(
        con.execute("SELECT order_id, currency FROM silver.orders WHERE tenant_id = 'initech'").fetchall()
    )
    assert rows[1] is None  # before the source migration
    assert rows[10] == "USD"  # after it
    currencies = {
        r[0]
        for r in con.execute(
            "SELECT currency FROM gold.tenant_daily_revenue WHERE tenant_id = 'initech'"
        ).fetchall()
    }
    assert currencies == {"USD"}


def test_gold_models(lake):
    _, _, con = lake
    # Orders 4 (refunded) and 7 (cancelled) are excluded: 10+20+30+50+60+80+95+100.
    assert one(con, "SELECT SUM(revenue) FROM gold.tenant_daily_revenue WHERE tenant_id='acme'") == 445
    customers = {
        r[0]
        for r in con.execute(
            "SELECT customer_id FROM gold.customer_lifetime_value WHERE tenant_id = 'acme'"
        ).fetchall()
    }
    assert customers == {1, 2, 3, 4, 6}  # deleted customer 5 excluded
    assert (
        one(
            con,
            "SELECT lifetime_value FROM gold.customer_lifetime_value "
            "WHERE tenant_id='acme' AND customer_id=4",
        )
        == 0
    )


def test_rerun_is_incremental_and_stable(lake):
    data, db, con = lake
    before = con.execute("SELECT * FROM silver.orders ORDER BY ALL").fetchall()
    con.close()
    con = pipeline.run(data, CONF, GOLD, db)
    assert con.execute("SELECT * FROM silver.orders ORDER BY ALL").fetchall() == before


def test_gold_model_name():
    from lakehouse.sql import gold_model_name

    assert gold_model_name("01_tenant_daily_revenue") == "tenant_daily_revenue"
    assert gold_model_name("adhoc_model") == "adhoc_model"
