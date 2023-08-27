"""Module adding support for reading/writing datasets as PyArrow Tables.

This is a core dependency of Pandas and Polars modules when dealing with
Parquet or Feather/ArrowIPC datasets. It provides full support for "Hive" style
partitioning.
"""

import tempfile

import pyarrow as pa
import pyarrow.dataset as pa_ds
import pyarrow.parquet as pq
from pyarrow.compute import field as pa_field
from pyarrow.compute import scalar as pa_scalar
from pyarrow.feather import read_table as pa_read_feather
from pyarrow.feather import write_feather as pa_write_feather

from ._fs import get_fs_path
from ._types import Dataset
from ._utils import func_arguments, get_parent, remove_extra_arguments


def read_feather(ds: Dataset) -> pa.Table:
    """Reads a Arrow IPC/Feather V2 dataset."""
    # get a fs, path reference
    fs, path = get_fs_path(ds)

    if fs.protocol == "file" and fs.isfile(path):
        # fast path for single file feather
        kwargs = dict()
        kwargs.update(ds.args)
        kwargs.update(ds.write_args)
        remove_extra_arguments(pa_read_feather, kwargs)
        return pa_read_feather(path, **kwargs)
    else:
        # support for more complex partitioned feather
        kwargs = dict()

        if ds.partition_by:
            kwargs["partitioning"] = "hive"

        if ds.partitions:
            _filter = pa_scalar(True)
            for key, val in ds.partitions.items():
                _filter &= pa_field(key) == val
            kwargs["filter"] = _filter

        kwargs.update(ds.args)
        kwargs.update(ds.write_args)
        kwargs["source"] = path
        kwargs["filesystem"] = fs
        kwargs["format"] = "feather"

        # pick dataset arguments
        dataset_args = kwargs.copy()
        remove_extra_arguments(pa_ds.dataset, dataset_args)

        # pick table arguments
        table_arg_set = ["columns", "filter", "batch_size"]
        table_args = {k: v for k, v in kwargs.items() if k in table_arg_set}

        return pa_ds.dataset(**dataset_args).to_table(**table_args)


def read_parquet(ds: Dataset) -> pa.Table:
    """Read a Parquet dataset."""
    # get a fs, path reference
    fs, path = get_fs_path(ds)

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

    remove_extra_arguments(pq.read_table, kwargs)

    return pq.read_table(source=path, filesystem=fs, **kwargs)


def write_parquet(tbl: pa.Table, ds: Dataset):
    """Writes a Parquet dataset."""
    func_args = func_arguments(pq.write_table)

    # handle partitions using dataset
    if ds.partition_by:
        return _write_dataset(tbl, ds, pa_ds.ParquetFileFormat(), func_args)

    fs, path = get_fs_path(ds)

    kwargs = dict()
    kwargs.update(ds.args)
    kwargs.update(ds.read_args)
    remove_extra_arguments(pq.write_table, kwargs)

    fs.makedirs(get_parent(path), exist_ok=True)
    pq.write_table(tbl, path, filesystem=fs, **kwargs)


def write_feather(tbl: pa.Table, ds: Dataset) -> None:
    """Writes a Arrow IPC/Feather V2 dataset.

    It defaults to uncompressed data.
    """
    # handle partitions using dataset
    if ds.partition_by:
        func_args = func_arguments(pq.ParquetWriter.__init__)
        func_args -= {"self", "where", "schema"}
        ds.args.setdefault("compression", "uncompressed")
        return _write_dataset(tbl, ds, pa_ds.IpcFileFormat(), func_args)

    fs, path = get_fs_path(ds)

    kwargs = dict()
    kwargs.update(ds.args)
    kwargs.update(ds.read_args)

    if fs.protocol == "file":
        remove_extra_arguments(pa_write_feather, kwargs)
        fs.makedirs(get_parent(path), exist_ok=True)
        pa_write_feather(tbl, path, **kwargs)
    else:
        with tempfile.TemporaryDirectory() as td:
            lpath = f"{td}/data"
            pa_write_feather(tbl, lpath, **kwargs)
            fs.put_file(lpath, path)


def _write_dataset(
    tbl: pa.Table, ds: Dataset, pa_fmt: pa_ds.FileFormat, write_options_set: set[str]
):
    # get a fs, path reference
    fs, path = get_fs_path(ds)

    # process arguments
    kwargs = dict()

    if ds.partition_by:
        kwargs["partitioning_flavor"] = "hive"
        kwargs["partitioning"] = ds.partition_by

    kwargs.update(ds.args)
    kwargs.update(ds.write_args)

    # pass feasible argument to parquet writer options
    writer_args = {k: v for k, v in kwargs.items() if k in write_options_set}
    file_options = pa_fmt.make_write_options(**writer_args)

    # get set of write_dataset options
    write_ds_options = kwargs.copy()
    remove_extra_arguments(pa_ds.write_dataset, write_ds_options)

    pa_ds.write_dataset(
        tbl,
        path,
        filesystem=fs,
        format=ds.format,
        file_options=file_options,
        **write_ds_options,
    )
