"""Table specifications: the contract between DMS source tables and silver Iceberg tables."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Tuple

# SQL is generated from these names, so they are validated strictly up front.
IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")
TENANT_ID = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
ALLOWED_TYPES = re.compile(r"^(STRING|BIGINT|INT|DOUBLE|BOOLEAN|DATE|TIMESTAMP|DECIMAL\(\d{1,2},\d{1,2}\))$")

# Metadata columns added to every silver table.
TENANT_COL = "tenant_id"
TS_COL = "_dms_ts"
FILE_COL = "_src_file"
DELETED_COL = "_is_deleted"


class SpecError(ValueError):
    pass


def check_identifier(name: str) -> str:
    if not IDENTIFIER.match(name):
        raise SpecError(f"invalid identifier: {name!r}")
    return name


def check_tenant(tenant: str) -> str:
    if not TENANT_ID.match(tenant):
        raise SpecError(f"invalid tenant id: {tenant!r}")
    return tenant


@dataclass(frozen=True)
class TableSpec:
    name: str
    source_schema: str
    primary_key: Tuple[str, ...]
    columns: Dict[str, str] = field(hash=False)

    def __post_init__(self) -> None:
        check_identifier(self.name)
        check_identifier(self.source_schema)
        if not self.primary_key:
            raise SpecError(f"{self.name}: primary_key is required")
        for col, typ in self.columns.items():
            check_identifier(col)
            if not ALLOWED_TYPES.match(typ):
                raise SpecError(f"{self.name}.{col}: unsupported type {typ!r}")
        missing = [k for k in self.primary_key if k not in self.columns]
        if missing:
            raise SpecError(f"{self.name}: primary key columns not in columns: {missing}")
        reserved = {TENANT_COL, TS_COL, FILE_COL, DELETED_COL} & set(self.columns)
        if reserved:
            raise SpecError(f"{self.name}: reserved column names used: {sorted(reserved)}")

    @property
    def merge_keys(self) -> Tuple[str, ...]:
        """Source primary keys are only unique within one tenant database."""
        return (TENANT_COL,) + self.primary_key

    @property
    def silver_columns(self) -> Dict[str, str]:
        cols = {TENANT_COL: "STRING"}
        cols.update(self.columns)
        cols.update({TS_COL: "TIMESTAMP", FILE_COL: "STRING", DELETED_COL: "BOOLEAN"})
        return cols


def load_specs(path: str | Path) -> Dict[str, TableSpec]:
    return parse_specs(json.loads(Path(path).read_text()))


def parse_specs(raw: dict) -> Dict[str, TableSpec]:
    return {
        name: TableSpec(
            name=name,
            source_schema=body["source_schema"],
            primary_key=tuple(body["primary_key"]),
            columns=dict(body["columns"]),
        )
        for name, body in raw.items()
    }
