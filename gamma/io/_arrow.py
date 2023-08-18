"""Module adding support for reading/writing datasets as PyArrow Tables.

This is a core dependency of Pandas and Polars modules when dealing with
Parquet or Feather/ArrowIPC datasets. It provides full support for "Hive" style
partitioning.
"""

import pyarrow as pa
import pyarrow.dataset as pa_ds

from . import dispatch
from ._fs import get_fs_path
from ._types import Dataset


def is_arrow_readable(ds: Dataset) -> bool:
    return ds.format in ("parquet", "feather", "arrow")


def is_arrow_writeable(ds: Dataset) -> bool:
    return ds.format in ("parquet", "feather", "arrow")


@dispatch
def read_arrow_table(ds: Dataset) -> pa.Table:
    return read_arrow_table(ds, ds.format, ds.protocol)


@dispatch
def read_arrow_table(ds: Dataset, fmt, protocol) -> pa.Table:
    arrow_ds = _build_arrow_dataset(ds, fmt)

    kwargs = {}
    if ds.columns:
        kwargs["columns"] = ds.columns

    if ds.partitions:
        _filter = [(key, "=", val) for key, val in ds.partitions.items()]
        kwargs["filter"] = _filter

    kwargs.update(ds.args)
    kwargs.update(ds.read_args)
    return arrow_ds.to_table(**kwargs)


@dispatch
def write_arrow_table(tb: pa.Table, ds: Dataset) -> None:
    return write_arrow_table(tb, ds, ds.format, ds.protocol)


@dispatch
def write_arrow_table(tb: pa.Table, ds: Dataset, fmt, protocol) -> None:
    fs, path = get_fs_path(ds)

    file_options = _get_pyarrow_dataset_writer_options(ds, fmt)

    pa_ds.write_dataset(
        data=tb,
        partitioning=ds.partition_by if ds.partition_by else None,
        partitioning_flavor="hive" if ds.partition_by else None,
        format=fmt,
        base_dir=path,
        filesystem=fs,
        file_options=file_options,
        existing_data_behavior="delete_matching",
        create_dir=True,
    )


def _get_pyarrow_dataset_writer_options(ds: Dataset, fmt):
    kwargs = {}
    if ds.compression:
        kwargs["compression"] = ds.compression

    kwargs.update(ds.args)
    kwargs.update(ds.write_args)

    match fmt:
        case "parquet":
            file_format = pa_ds.ParquetFileFormat()
        case "arrow" | "feather":
            file_format = pa_ds.IpcFileFormat()

    return file_format.make_write_options(**kwargs)


def _build_arrow_dataset(ds: Dataset, fmt):
    fs, path = get_fs_path(ds)

    # parse pyarrrow dataset arguments
    kwargs = dict(source=path, filesystem=fs, format=fmt)

    if ds.partition_by:
        kwargs["partitioning"] = "hive"

    # load it
    arrow_ds = pa_ds.dataset(**kwargs)

    return arrow_ds
