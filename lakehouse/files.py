"""Bronze file layout, incremental file selection and tenant batching.

Layout written by one DMS S3 target endpoint per source database
(``BucketFolder=<prefix>tenant=<id>``)::

    bronze/tenant=<id>/<schema>/<table>/LOAD00000001.parquet      full load
    bronze/tenant=<id>/<schema>/<table>/20261017-093000123.parquet CDC, time-ordered
"""

from __future__ import annotations

import posixpath
import zlib
from typing import Iterable, List, Optional, Sequence, Tuple

from .spec import check_tenant

TENANT_PREFIX = "tenant="
LOAD_DONE = "LOAD"  # watermark meaning "full load processed, no CDC yet"


def table_prefix(bronze_prefix: str, tenant: str, schema: str, table: str) -> str:
    base = bronze_prefix.rstrip("/")
    return f"{base}/{TENANT_PREFIX}{check_tenant(tenant)}/{schema}/{table}/"


def tenant_from_prefix(prefix: str) -> Optional[str]:
    """``bronze/tenant=acme/`` -> ``acme``; anything else -> None."""
    last = prefix.rstrip("/").rsplit("/", 1)[-1]
    if not last.startswith(TENANT_PREFIX):
        return None
    tenant = last[len(TENANT_PREFIX) :]
    try:
        return check_tenant(tenant)
    except ValueError:
        return None


def is_full_load(key: str) -> bool:
    return posixpath.basename(key).startswith("LOAD")


def select_new_files(keys: Iterable[str], watermark: Optional[str]) -> Tuple[List[str], Optional[str]]:
    """Return (files to process, new watermark).

    DMS CDC file names sort in commit order, so the watermark is simply the last CDC
    file name processed. Full-load files (``LOAD*``) sort *after* CDC names, so they are
    handled explicitly: processed once, on the first run.
    """
    parquet = [k for k in keys if k.endswith(".parquet")]
    loads = sorted(k for k in parquet if is_full_load(k))
    cdc = sorted((k for k in parquet if not is_full_load(k)), key=posixpath.basename)

    if watermark is None:
        selected = loads + cdc
    else:
        after = "" if watermark == LOAD_DONE else watermark
        selected = [k for k in cdc if posixpath.basename(k) > after]

    if not selected:
        return [], watermark
    cdc_selected = [k for k in selected if not is_full_load(k)]
    new_watermark = posixpath.basename(cdc_selected[-1]) if cdc_selected else LOAD_DONE
    return selected, new_watermark


def batch_of(tenant: str, batch_count: int) -> int:
    """Stable tenant -> batch assignment (stable across runs and Python processes)."""
    return zlib.crc32(tenant.encode("utf-8")) % batch_count


def select_batch(tenants: Sequence[str], batch_index: int, batch_count: int) -> List[str]:
    """Split tenants into ``batch_count`` disjoint batches.

    Each Glue run takes one batch, so the number of parallel runs (bounded by the
    Glue job's MaxConcurrentRuns) is decoupled from the number of source databases.
    """
    if batch_count < 1 or not 0 <= batch_index < batch_count:
        raise ValueError(f"invalid batch {batch_index}/{batch_count}")
    return sorted(t for t in set(tenants) if batch_of(t, batch_count) == batch_index)
