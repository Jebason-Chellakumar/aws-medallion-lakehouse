"""Generate DMS-style bronze files (full load + CDC) for a few tenant databases.

Each tenant is one source database, as DMS writes it to S3 with
``DataFormat=parquet, IncludeOpForFullLoad=true, TimestampColumnName=_dms_ts``.
The scenario deliberately includes the hard cases: repeated updates to one row in a
single batch, two changes with the same commit timestamp, deletes, and a legacy tenant
whose schema is missing a column until a later migration adds it.

    python -m local_lake.sample_data --out data
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta
from pathlib import Path

import duckdb

TENANTS = {
    "acme": {"currency": "USD", "legacy": False},
    "globex": {"currency": "EUR", "legacy": False},
    "initech": {"currency": None, "legacy": True},  # orders table has no currency column yet
}
COUNTRIES = ["IN", "US", "DE", "GB", "SG"]
LOAD_TS = datetime(2026, 10, 1, 0, 0, 0)
CDC_FILES = {
    "20261001-120000000.parquet": datetime(2026, 10, 1, 12, 0, 0),
    "20261002-120000000.parquet": datetime(2026, 10, 2, 12, 0, 0),
}

CUSTOMER_COLS = [
    ("Op", "VARCHAR"),
    ("_dms_ts", "VARCHAR"),
    ("customer_id", "BIGINT"),
    ("name", "VARCHAR"),
    ("email", "VARCHAR"),
    ("country", "VARCHAR"),
    ("created_at", "TIMESTAMP"),
    ("updated_at", "TIMESTAMP"),
]
ORDER_COLS = [
    ("Op", "VARCHAR"),
    ("_dms_ts", "VARCHAR"),
    ("order_id", "BIGINT"),
    ("customer_id", "BIGINT"),
    ("status", "VARCHAR"),
    ("amount", "DECIMAL(12,2)"),
    ("currency", "VARCHAR"),
    ("ordered_at", "TIMESTAMP"),
    ("updated_at", "TIMESTAMP"),
]


def _ts(t: datetime) -> str:
    # DMS writes the commit timestamp as a string with microseconds.
    return t.strftime("%Y-%m-%d %H:%M:%S.%f")


def _customer(tenant: str, i: int, op: str, ts: datetime) -> dict:
    return {
        "Op": op,
        "_dms_ts": _ts(ts),
        "customer_id": i,
        "name": f"{tenant.title()} Customer {i}",
        "email": f"c{i}@{tenant}.example",
        "country": COUNTRIES[(i - 1) % len(COUNTRIES)],
        "created_at": LOAD_TS - timedelta(days=30 - i),
        "updated_at": ts,
    }


def _order(cfg: dict, i: int, op: str, ts: datetime, **overrides) -> dict:
    row = {
        "Op": op,
        "_dms_ts": _ts(ts),
        "order_id": i,
        "customer_id": (i - 1) % 5 + 1,
        "status": "pending" if i in (3, 4) else "delivered",
        "amount": i * 10,
        "currency": cfg["currency"],
        "ordered_at": datetime(2026, 9, 28 + i % 3, 10, i),
        "updated_at": ts,
    }
    row.update(overrides)
    return row


def _write(con, path: Path, columns, rows, drop=()) -> None:
    cols = [(n, t) for n, t in columns if n not in drop]
    path.parent.mkdir(parents=True, exist_ok=True)
    con.execute("CREATE OR REPLACE TEMP TABLE _rows (" + ", ".join(f"{n} {t}" for n, t in cols) + ")")
    con.executemany(
        "INSERT INTO _rows VALUES (" + ", ".join("?" for _ in cols) + ")",
        [[r[n] for n, _ in cols] for r in rows],
    )
    con.execute(f"COPY _rows TO '{path.as_posix()}' (FORMAT parquet)")


def generate(out: Path) -> None:
    con = duckdb.connect()
    cdc1, cdc2 = list(CDC_FILES)
    t1, t2 = CDC_FILES[cdc1], CDC_FILES[cdc2]
    for tenant, cfg in TENANTS.items():
        base = out / "bronze" / f"tenant={tenant}" / "public"
        legacy_drop = ("currency",) if cfg["legacy"] else ()
        s = timedelta(seconds=1)

        _write(
            con,
            base / "customers" / "LOAD00000001.parquet",
            CUSTOMER_COLS,
            [_customer(tenant, i, "I", LOAD_TS) for i in range(1, 6)],
        )
        _write(
            con,
            base / "orders" / "LOAD00000001.parquet",
            ORDER_COLS,
            [_order(cfg, i, "I", LOAD_TS) for i in range(1, 9)],
            drop=legacy_drop,
        )

        # CDC batch 1: several changes to the same rows within one file.
        _write(con, base / "customers" / cdc1, CUSTOMER_COLS, [_customer(tenant, 5, "D", t1 + 4 * s)])
        _write(
            con,
            base / "orders" / cdc1,
            ORDER_COLS,
            [
                _order(cfg, 3, "U", t1 + 1 * s, status="shipped"),
                _order(cfg, 3, "U", t1 + 2 * s, status="delivered"),
                _order(cfg, 9, "I", t1 + 3 * s, customer_id=2, status="pending"),
                _order(cfg, 7, "U", t1 + 5 * s, status="cancelled"),
                # Same commit timestamp: the later row in the file must win.
                _order(cfg, 4, "U", t1 + 6 * s, status="shipped"),
                _order(cfg, 4, "U", t1 + 6 * s, status="refunded"),
            ],
            drop=legacy_drop,
        )

        # CDC batch 2: the legacy tenant has now migrated and sends currency too.
        currency = cfg["currency"] or "USD"
        _write(con, base / "customers" / cdc2, CUSTOMER_COLS, [_customer(tenant, 6, "I", t2)])
        _write(
            con,
            base / "orders" / cdc2,
            ORDER_COLS,
            [
                _order(cfg, 9, "U", t2 + 1 * s, customer_id=2, amount=95, currency=currency),
                _order(cfg, 10, "I", t2 + 2 * s, customer_id=6, amount=100, currency=currency),
            ],
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, default=Path("data"))
    args = parser.parse_args()
    generate(args.out)
    print(f"Wrote DMS-style bronze files for {len(TENANTS)} tenants to {args.out / 'bronze'}")


if __name__ == "__main__":
    main()
