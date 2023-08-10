import pytest

from gamma.io import Dataset, get_dataset


def test_dataset_partition():
    from gamma.io import PartitionException
    from gamma.io._dataset import _validate_partitions

    base = dict(
        layer="raw",
        name="clients",
        location="file:///tmp/ds",
        protocol="file",
        format="csv",
        partition_by=["year", "month", "country"],
    )
    # test valid 1
    ds = Dataset(**base, partitions={"year": "2022", "month": "11"})
    _validate_partitions(ds)

    # test valid no partition provided
    ds = Dataset(**base)
    _validate_partitions(ds)

    # test invalid - partition with wholes
    ds = Dataset(**base, partitions={"month": "11"})
    with pytest.raises(PartitionException):
        _validate_partitions(ds)

    # test invalid - partition with wholes 2
    ds = Dataset(
        **base,
        partitions={"country": "US"},
    )
    with pytest.raises(PartitionException):
        _validate_partitions(ds)


def test_load_ds_from_config(io_config):
    ds = get_dataset("source", "customers_1k")
    assert ds.location.startswith("https")
    assert ds.compression == "zip"

    ds = get_dataset("raw", "customers", cluster="A")
    assert ds.location.startswith("file")
    assert ds.partition_by == ["cluster"]
    assert ds.partitions == {"cluster": "A"}

    ds = get_dataset("raw", "customers")
    assert ds.location.startswith("file")
    assert ds.partition_by == ["cluster"]
    assert not ds.partitions
