"""Run the full bronze -> silver -> gold pipeline locally on DuckDB.

Uses the same SQL builders, file selection and watermark logic as the Glue jobs, so
the pipeline can be developed and tested without an AWS account.

    python -m local_lake.sample_data --out data
    python -m local_lake.pipeline --data data
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Optional

import duckdb

from lakehouse import files
from lakehouse import sql as q
from lakehouse.spec import TableSpec, load_specs

ROOT = Path(__file__).resolve().parent.parent


def _read_watermark(state_dir: Path, table: str, tenant: str) -> Optional[str]:
    path = state_dir / table / f"tenant={tenant}.json"
    return json.loads(path.read_text())["watermark"] if path.exists() else None


def _write_watermark(state_dir: Path, table: str, tenant: str, watermark: str) -> None:
    path = state_dir / table / f"tenant={tenant}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"watermark": watermark}))


def _sql_list(paths) -> str:
    return "[" + ", ".join("'" + p.replace("'", "''") + "'" for p in paths) + "]"


def bronze_to_silver(con, spec: TableSpec, data_dir: Path) -> int:
    """Merge new bronze files for one table across all tenants. Returns files processed."""
    bronze, state = data_dir / "bronze", data_dir / "_state"
    tenants = sorted(
        t for t in (files.tenant_from_prefix(p.name) for p in bronze.iterdir() if p.is_dir()) if t
    )
    target = f"silver.{spec.name}"
    con.execute(q.create_table_sql(spec, target, q.DUCKDB))

    parts, watermarks, processed = [], {}, 0
    for i, tenant in enumerate(tenants):
        prefix = Path(files.table_prefix(str(bronze), tenant, spec.source_schema, spec.name))
        keys = [p.as_posix() for p in prefix.glob("*.parquet")]
        selected, new_wm = files.select_new_files(keys, _read_watermark(state, spec.name, tenant))
        if not selected:
            continue
        view = f"raw_{i}"
        con.execute(
            f"CREATE OR REPLACE TEMP VIEW {view} AS "
            f"SELECT *, filename AS _src_file, file_row_number AS _row "
            f"FROM read_parquet({_sql_list(selected)}, union_by_name = true, "
            f"filename = true, file_row_number = true)"
        )
        columns = [row[0] for row in con.execute(f"DESCRIBE {view}").fetchall()]
        parts.append(q.projection_sql(spec, view, columns, tenant))
        watermarks[tenant] = new_wm
        processed += len(selected)

    if not parts:
        return 0
    con.execute("CREATE OR REPLACE TEMP VIEW staged_raw AS " + " UNION ALL ".join(parts))
    con.execute("CREATE OR REPLACE TEMP TABLE staged AS " + q.dedup_sql(spec, "staged_raw"))
    con.execute(q.merge_sql(spec, target, "staged", tenants=sorted(watermarks)))
    # Watermarks advance only after the merge succeeded: a failed run is simply retried.
    for tenant, wm in watermarks.items():
        _write_watermark(state, spec.name, tenant, wm)
    return processed


def silver_to_gold(con, gold_dir: Path) -> list:
    built = []
    for path in sorted(gold_dir.glob("*.sql")):
        name = q.gold_model_name(path.stem)
        con.execute(q.ctas_sql(f"gold.{name}", path.read_text(), q.DUCKDB))
        built.append(name)
    return built


def run(data_dir: Path, conf: Path, gold_dir: Path, db: str = ":memory:"):
    con = duckdb.connect(db)
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("CREATE SCHEMA IF NOT EXISTS gold")
    for spec in load_specs(conf).values():
        n = bronze_to_silver(con, spec, data_dir)
        print(f"silver.{spec.name}: merged {n} new file(s)")
    for name in silver_to_gold(con, gold_dir):
        print(f"gold.{name}: rebuilt")
    return con


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--data", type=Path, default=Path("data"))
    parser.add_argument("--conf", type=Path, default=ROOT / "conf" / "tables.json")
    parser.add_argument("--gold", type=Path, default=ROOT / "gold")
    parser.add_argument("--db", default=None, help="DuckDB file (default: <data>/lakehouse.duckdb)")
    args = parser.parse_args()
    con = run(args.data, args.conf, args.gold, args.db or str(args.data / "lakehouse.duckdb"))
    print()
    con.sql("SELECT * FROM gold.tenant_daily_revenue ORDER BY tenant_id, order_date").show()
    con.sql("SELECT * FROM gold.customer_lifetime_value ORDER BY tenant_id, customer_id").show()


if __name__ == "__main__":
    main()
