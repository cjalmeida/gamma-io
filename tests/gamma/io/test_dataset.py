import pytest

from gamma.io import Dataset, get_dataset


def test_load_ds_from_config(io_config):
    ds = get_dataset("source", "customers_1k")
    assert ds.location.startswith("https")
    assert ds.args["compression"] == "zip"

    ds = get_dataset("raw", "customers", l1="A")
    assert ds.location.startswith("file")
    assert ds.partition_by == ["l1", "l2"]
    assert ds.partitions == {"l1": "A"}

    ds = get_dataset("raw", "customers")
    assert ds.location.startswith("file")
    assert ds.partition_by == ["l1", "l2"]
    assert not ds.partitions
