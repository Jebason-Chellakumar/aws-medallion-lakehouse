import pytest

from lakehouse import files
from lakehouse.files import LOAD_DONE, select_batch, select_new_files, tenant_from_prefix

P = "bronze/tenant=acme/public/orders/"
LOAD = P + "LOAD00000001.parquet"
LOAD2 = P + "LOAD00000002.parquet"
C1 = P + "20261001-120000000.parquet"
C2 = P + "20261002-120000000.parquet"


def test_first_run_takes_full_load_then_cdc_in_order():
    selected, wm = select_new_files([C2, LOAD2, C1, LOAD], None)
    assert selected == [LOAD, LOAD2, C1, C2]
    assert wm == "20261002-120000000.parquet"


def test_full_load_only_sets_load_done_watermark():
    selected, wm = select_new_files([LOAD], None)
    assert selected == [LOAD]
    assert wm == LOAD_DONE


def test_after_load_done_only_cdc_files_are_new():
    selected, wm = select_new_files([LOAD, C1], LOAD_DONE)
    assert selected == [C1]
    assert wm == "20261001-120000000.parquet"


def test_incremental_run_skips_processed_files_and_load():
    selected, wm = select_new_files([LOAD, C1, C2], "20261001-120000000.parquet")
    assert selected == [C2]
    assert wm == "20261002-120000000.parquet"


def test_nothing_new_keeps_watermark():
    assert select_new_files([LOAD, C1], "20261001-120000000.parquet") == ([], "20261001-120000000.parquet")


def test_ignores_non_parquet_objects():
    selected, _ = select_new_files([P + "_SUCCESS", P + "notes.txt", C1], None)
    assert selected == [C1]


@pytest.mark.parametrize(
    "prefix, expected",
    [
        ("bronze/tenant=acme/", "acme"),
        ("bronze/tenant=tenant-042", "tenant-042"),
        ("bronze/other/", None),
        ("bronze/tenant=bad'name/", None),
    ],
)
def test_tenant_from_prefix(prefix, expected):
    assert tenant_from_prefix(prefix) == expected


def test_table_prefix():
    assert files.table_prefix("bronze/", "acme", "public", "orders") == P


def test_batches_are_disjoint_complete_and_stable():
    tenants = [f"tenant{i:03d}" for i in range(600)]
    batches = [select_batch(tenants, i, 8) for i in range(8)]
    flat = [t for b in batches for t in b]
    assert sorted(flat) == sorted(tenants)
    assert len(flat) == len(set(flat))
    assert all(len(b) > 40 for b in batches)  # reasonably even
    assert batches == [select_batch(tenants, i, 8) for i in range(8)]


@pytest.mark.parametrize("index, count", [(0, 0), (4, 4), (-1, 4)])
def test_invalid_batch(index, count):
    with pytest.raises(ValueError):
        select_batch(["a"], index, count)
