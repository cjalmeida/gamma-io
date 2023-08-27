import pytest

from gamma.io import Dataset, get_dataset, get_dataset_location


def test_load_ds_from_config(io_config):
    ds = get_dataset("source", "customers_1k")
    assert ds.location.startswith("https")
    assert ds.args["compression"] == "zip"

    ds = get_dataset("raw", "customers_parquet", l1="A")
    assert ds.location.startswith("file")
    assert ds.partition_by == ["l1", "l2"]
    assert ds.partitions == {"l1": "A"}

    ds = get_dataset("raw", "customers_parquet")
    assert ds.location.startswith("file")
    assert ds.partition_by == ["l1", "l2"]
    assert not ds.partitions


def test_location(io_config):
    ds = Dataset(
        layer="foo",
        name="data",
        location="file:///tmp/data/",
        format="parquet",
    )

    # check is folder 1
    loc = get_dataset_location(ds)
    ds.is_file = None
    assert loc.endswith("/")

    # check is folder 2
    ds.location = "file:///tmp/data"
    ds.is_file = None
    loc = get_dataset_location(ds)
    assert loc.endswith("/")

    # check is folder 3
    ds.location = "file:///tmp/data.foo"
    ds.is_file = False
    loc = get_dataset_location(ds)
    assert loc.endswith("/")

    # check is folder 4
    ds.location = "file:///tmp/data.foo/"
    ds.is_file = None
    loc = get_dataset_location(ds)
    assert loc.endswith("/")

    # check is file 1
    ds.location = "file:///tmp/data.foo"
    ds.is_file = None
    loc = get_dataset_location(ds)
    assert not loc.endswith("/")

    # check is file 2
    ds.location = "file:///tmp/data"
    ds.is_file = True
    loc = get_dataset_location(ds)
    assert not loc.endswith("/")
