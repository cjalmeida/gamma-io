from gamma.io import Dataset
import pytest


def test_dataset_partition():
    from gamma.io._core import _validate_partitions
    from gamma.io import PartitionException

    # test valid 1
    ds = Dataset(
        layer="raw",
        name="clients",
        location="file:///tmp/ds",
        partition_by=["year", "month", "country"],
        partitions={"year": "2022", "month": "11"},
    )

    _validate_partitions(ds)

    # test valid no partition provided
    ds = Dataset(
        layer="raw",
        name="clients",
        location="file:///tmp/ds",
        partition_by=["year", "month", "country"],
    )

    # test invalid - partition with wholes
    ds = Dataset(
        layer="raw",
        name="clients",
        location="file:///tmp/ds",
        partition_by=["year", "month", "country"],
        partitions={"month": "11"},
    )
    with pytest.raises(PartitionException):
        _validate_partitions(ds)

    # test invalid - partition with wholes 2
    ds = Dataset(
        layer="raw",
        name="clients",
        location="file:///tmp/ds",
        partition_by=["year", "month", "country"],
        partitions={"country": "US"},
    )
    with pytest.raises(PartitionException):
        _validate_partitions(ds)
