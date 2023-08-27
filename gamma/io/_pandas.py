from typing import Literal, Type

import pandas as pd
from fsspec import AbstractFileSystem

from . import dispatch
from ._dataset import get_dataset, get_extension
from ._logging import log_ds_read, log_ds_write
from ._staging import get_stage_reader_fs_path, get_stage_writer_fs_path
from ._types import ArrowFmt, Dataset
from ._utils import get_parent, get_single_file_in_folder, remove_extra_arguments


@dispatch
def read_dataset(cls: Type[pd.DataFrame], *args, **kwargs) -> pd.DataFrame:
    return read_pandas(*args, **kwargs)


@dispatch
def read_dataset(cls: Type[pd.DataFrame], ds: Dataset) -> pd.DataFrame:
    return read_pandas(ds)


@dispatch
def write_dataset(df: pd.DataFrame, *args, **kwargs) -> None:
    return write_pandas(df, *args, **kwargs)


@dispatch
def write_dataset(df: pd.DataFrame, ds: Dataset) -> None:
    return write_pandas(df, ds)


@dispatch
def read_pandas(*args, **kwargs) -> pd.DataFrame:
    """Pandas dataset reader shortcut."""
    return read_pandas(get_dataset(*args, **kwargs))


@dispatch
@log_ds_read
def read_pandas(ds: Dataset):
    """Pandas dataset reader shortcut."""
    return read_pandas(ds, ds.format, ds.protocol)


@dispatch
def read_pandas(ds: Dataset, fmt, protocol):
    """Fallback reader for any format and storage protocol.

    We assume the storage to be `fsspec` stream compatible (ie. single file).
    """
    func = _get_reader(ds, fmt)
    kwargs = _get_reader_arguments(ds, func)

    # get a fs, path reference
    fs, path = get_stage_reader_fs_path(ds)

    path = _adjust_reader_path_pandas(ds, fs, path, fmt)

    # stream and read data
    with fs.open(path, "rb") as fo:
        return func(fo, **kwargs)


@dispatch
def _adjust_reader_path_pandas(ds: Dataset, fs: AbstractFileSystem, path: str, fmt):
    """Adjust the path argument to read a single file, as expected by most readers."""
    if fs.isdir(path):
        path = get_single_file_in_folder(fs, path, ds)

    return path


def _get_reader(ds: Dataset, fmt):
    # get reader function based on format name
    func = getattr(pd, f"read_{fmt}", None)
    if func is None:
        NotImplemented(f"Format not supported {fmt}")
    return func


def _get_reader_arguments(ds: Dataset, func):
    # process arguments
    kwargs = dict()
    kwargs.update(ds.args)
    kwargs.update(ds.read_args)

    remove_extra_arguments(func, kwargs)
    return kwargs


@dispatch
def read_pandas(ds: Dataset, fmt: Literal["parquet"], protocol) -> pd.DataFrame:
    """Specialized support for Parquet datasets."""
    from ._arrow import read_parquet

    tbl = read_parquet(ds)
    return tbl.to_pandas()


@dispatch
def read_pandas(ds: Dataset, fmt: ArrowFmt, protocol) -> pd.DataFrame:
    from ._arrow import read_feather

    tbl = read_feather(ds)
    return tbl.to_pandas()


@dispatch
def write_pandas(df: pd.DataFrame, *args, **kwargs) -> None:
    ds = get_dataset(*args, **kwargs)
    return write_pandas(df, ds)


@dispatch
@log_ds_write
def write_pandas(df: pd.DataFrame, ds: Dataset) -> None:
    """Write a pandas DataFrame to a dataset."""
    return write_pandas(df, ds, ds.format, ds.protocol)


@dispatch
def write_pandas(df: pd.DataFrame, ds: Dataset, fmt, protocol):
    """Fallback writer for writing pandas Dataframe to a dataset.

    We assume the storage to be `fsspec` stream compatible (ie. single file).
    """
    # get reader function based on format name
    func = getattr(pd.DataFrame, f"to_{fmt}", None)
    if func is None:
        NotImplemented(f"Format not supported {fmt}")

    # get a fs, path reference
    fs, path = get_stage_writer_fs_path(ds)

    # process arguments
    kwargs = process_write_args(ds, fmt)
    remove_extra_arguments(func, kwargs)

    if fs.exists(path):
        fs.rm(path, recursive=True)

    path = _adjust_writer_path_pandas(ds, fs, path, fmt)

    # write data as stream
    fs.makedirs(get_parent(path), exist_ok=True)
    with fs.open(path, "wb") as fo:
        return func(df, fo, **kwargs)


def _adjust_writer_path_pandas(ds, fs, path: str, fmt):
    """Adjust the path when writing to folder."""
    ext = get_extension(fmt)
    if path.endswith("/"):
        return f"{path}data.{ext}"
    return path


@dispatch
def write_pandas(df: pd.DataFrame, ds: Dataset, fmt: Literal["parquet"], proto) -> None:
    import pyarrow as pa

    from ._arrow import write_parquet

    tbl = pa.Table.from_pandas(df, preserve_index=False)
    write_parquet(tbl, ds)


@dispatch
def write_pandas(df: pd.DataFrame, ds: Dataset, fmt: ArrowFmt, proto) -> None:
    import pyarrow as pa

    from ._arrow import write_feather

    tbl = pa.Table.from_pandas(df, preserve_index=False)
    write_feather(tbl, ds)


@dispatch
def process_write_args(ds: Dataset, fmt):
    """Process dataset writer arguments."""
    kwargs = {}
    kwargs.update(ds.args)
    kwargs.update(ds.write_args)
    return kwargs


@dispatch
def process_write_args(ds: Dataset, fmt: Literal["csv"]):
    """Process dataset writer arguments for CSVs."""
    kwargs = dict(index=False)
    kwargs.update(ds.args)
    kwargs.update(ds.write_args)
    return kwargs


@dispatch
def list_partitions(*args, **kwargs) -> pd.DataFrame:
    """List the existing partition set.

    Return a Dataframe with the available partitions and size.
    """
    ds = get_dataset(*args, **kwargs)
    return list_partitions(ds)


@dispatch
def list_partitions(ds: Dataset, **kwargs) -> pd.DataFrame:
    """List the existing partition set.

    Return a Dataframe with the available partitions and size.
    """
    ds = get_dataset(layer=ds.layer, name=ds.name, **kwargs)
    ds.args.update({"columns": ds.partition_by})

    return read_pandas(ds).drop_duplicates()
