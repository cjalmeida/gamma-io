"""IO support for Polars."""


from typing import Literal, Type

import polars as pl
from fsspec import AbstractFileSystem

from . import dispatch
from ._dataset import get_dataset, get_extension
from ._logging import log_ds_read, log_ds_write
from ._staging import get_stage_reader_fs_path, get_stage_writer_fs_path
from ._types import ArrowFmt, Dataset
from ._utils import get_parent, get_single_file_in_folder, remove_extra_arguments


@dispatch
def read_dataset(cls: Type[pl.DataFrame], *args, **kwargs) -> pl.DataFrame:
    return read_polars(*args, **kwargs)


@dispatch
def read_dataset(cls: Type[pl.DataFrame], ds: Dataset) -> pl.DataFrame:
    return read_polars(ds)


@dispatch
def write_dataset(df: pl.DataFrame, *args, **kwargs) -> None:
    return write_polars(df, *args, **kwargs)


@dispatch
def write_dataset(df: pl.DataFrame, ds: Dataset) -> None:
    return write_polars(df, ds)


@dispatch
def read_polars(*args, **kwargs) -> pl.DataFrame:
    """Polars dataset reader shortcut."""
    return read_polars(get_dataset(*args, **kwargs))


@dispatch
@log_ds_read
def read_polars(ds: Dataset):
    """Polars dataset reader shortcut."""
    return read_polars(ds, ds.format, ds.protocol)


@dispatch
def read_polars(ds: Dataset, fmt, protocol):
    """Fallback reader for any format and storage protocol.

    We assume the storage to be `fsspec` stream compatible (ie. single file).
    """
    # get reader function based on format name
    func = getattr(pl, f"read_{fmt}", None)
    if func is None:  # pragma: no cover
        ValueError(f"Reading Polars format not supported yet: {fmt}")

    # get a fs, path reference
    fs, path = get_stage_reader_fs_path(ds)

    # process arguments
    kwargs = dict()
    kwargs.update(ds.args)
    kwargs.update(ds.read_args)
    remove_extra_arguments(func, kwargs)

    path = _adjust_reader_path_polars(ds, fs, path, fmt)

    # stream and read data
    with fs.open(path, "rb") as fo:
        return func(fo, **kwargs)


@dispatch
def read_polars(ds: Dataset, fmt: Literal["parquet"], protocol):
    """Read Parquet into a DataFrame."""
    from ._arrow import read_parquet

    tbl = read_parquet(ds)
    return pl.from_arrow(tbl)


@dispatch
def read_polars(ds: Dataset, fmt: ArrowFmt, proto):
    """Read Arrow IPC/Feather into a DataFrame."""
    from ._arrow import read_feather

    tbl = read_feather(ds)
    return pl.from_arrow(tbl)


@dispatch
def write_polars(df: pl.DataFrame, *args, **kwargs) -> None:
    ds = get_dataset(*args, **kwargs)
    return write_polars(df, ds)


@dispatch
@log_ds_write
def write_polars(df: pl.DataFrame, ds: Dataset) -> None:
    """Write a polars DataFrame to a dataset."""
    return write_polars(df, ds, ds.format, ds.protocol)


@dispatch
def write_polars(df: pl.DataFrame, ds: Dataset, fmt, protocol):
    """We assume the storage to be `fsspec` stream compatible (ie. single file)."""
    # get reader function based on format name
    func = getattr(pl.DataFrame, f"write_{fmt}", None)
    if func is None:  # pragma: no cover
        ValueError(f"Writing Polars format not supported yet: {fmt}")

    # get a fs, path reference
    fs, path = get_stage_writer_fs_path(ds)

    # process arguments
    kwargs = dict()
    kwargs.update(ds.args)
    kwargs.update(ds.write_args)

    remove_extra_arguments(func, kwargs)

    path = _adjust_writer_path_polars(ds, fs, path, fmt)

    # stream and write data
    fs.makedirs(get_parent(path), exist_ok=True)
    with fs.open(path, "wb") as fo:
        return func(df, fo, **kwargs)


@dispatch
def write_polars(df: pl.DataFrame, ds: Dataset, fmt: Literal["parquet"], proto) -> None:
    """Write DataFrame as Parquet."""
    from ._arrow import write_parquet

    write_parquet(df.to_arrow(), ds)


@dispatch
def write_polars(df: pl.DataFrame, ds: Dataset, fmt: ArrowFmt, proto) -> None:
    from ._arrow import write_feather

    write_feather(df.to_arrow(), ds)


def _adjust_writer_path_polars(ds, fs, path: str, fmt):
    """Adjust the path when writing to folder."""
    ext = get_extension(fmt)
    if path.endswith("/"):
        return f"{path}data.{ext}"
    return path


@dispatch
def _adjust_reader_path_polars(ds: Dataset, fs: AbstractFileSystem, path: str, fmt):
    """Adjust the path argument to read a single file, as expected by most readers."""
    if fs.isdir(path):
        path = get_single_file_in_folder(fs, path, ds)

    return path
