"""Module adding support for reading/writing datasets as PyArrow Tables.

This is a core dependency of Pandas and Polars modules when dealing with
Parquet or Feather/ArrowIPC datasets. It provides full support for "Hive" style
partitioning.
"""

from pyarrow.compute import field as pa_field
from pyarrow.compute import scalar as pa_scalar

from ._types import Dataset


def process_arrow_read_args(ds: Dataset):
    """Process dataset reader arguments for pyarrow engine."""
    kwargs = {}

    if ds.partition_by:
        kwargs["partitioning"] = "hive"

    if ds.partitions:
        _filter = pa_scalar(True)
        for key, val in ds.partitions.items():
            _filter &= pa_field(key) == val
        kwargs["filters"] = _filter

    kwargs.update(ds.args)
    kwargs.update(ds.read_args)
    return kwargs


def process_arrow_write_args(ds: Dataset):
    """Process dataset write arguments for formats supported by PyArrow."""
    kwargs = dict(compression=ds.compression)

    if ds.partition_by:
        kwargs["partition_cols"] = ds.partition_by

    kwargs.update(ds.args)
    kwargs.update(ds.write_args)

    return kwargs
