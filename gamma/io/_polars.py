"""IO support for Polars."""


import polars as pl

from . import dispatch
from ._arrow import (
    is_arrow_readable,
    is_arrow_writeable,
    read_arrow_table,
    write_arrow_table,
)
from ._dataset import get_dataset
from ._fs import get_fs_path
from ._logging import log_ds_read, log_ds_write
from ._types import Dataset


@dispatch
def read_polars(*args, **kwargs) -> pl.DataFrame:
    """Polars dataset reader shortcut."""
    return read_polars(get_dataset(*args, **kwargs))


@dispatch
@log_ds_read
def read_polars(ds: Dataset):
    """Polars dataset reader shortcut."""
    # Delegate to pyarrow if supported
    if is_arrow_readable(ds):
        tb = read_arrow_table(ds)
        return pl.from_arrow(tb)

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
    fs, path = get_fs_path(ds)

    # process arguments
    kwargs = _process_read_args(ds)

    # stream and read data
    with fs.open(path, "rb") as fo:
        return func(fo, **kwargs)


def _process_read_args(ds: Dataset):
    """Process dataset reader/writer arguments.

    We parse `compression`, `columns` top-level args, and merge with `args` /
    `reader_args`.
    """
    kwargs = {}
    if ds.compression:
        kwargs["compression"] = ds.compression

    if ds.columns:
        kwargs["columns"] = ds.columns

    kwargs.update(ds.args)
    kwargs.update(ds.read_args)
    return kwargs


@dispatch
def write_polars(df: pl.DataFrame, *args, **kwargs) -> None:
    ds = get_dataset(*args, **kwargs)
    return write_polars(df, ds)


@dispatch
@log_ds_write
def write_polars(df: pl.DataFrame, ds: Dataset) -> None:
    """Write a polars DataFrame to a dataset."""
    # Delegate to pyarrow if supported
    if is_arrow_writeable(ds):
        tb = df.to_arrow()
        return write_arrow_table(tb, ds)

    return write_polars(df, ds, ds.format, ds.protocol)


@dispatch
def write_polars(df: pl.DataFrame, ds: Dataset, fmt, protocol):
    """We assume the storage to be `fsspec` stream compatible (ie. single file)."""
    # get reader function based on format name
    func = getattr(pl.DataFrame, f"write_{fmt}", None)
    if func is None:  # pragma: no cover
        ValueError(f"Writing Polars format not supported yet: {fmt}")

    # get a fs, path reference
    fs, path = get_fs_path(ds)

    # process arguments
    kwargs = _process_write_args(ds)

    # stream and read data
    with fs.open(path, "wb") as fo:
        return func(df, fo, **kwargs)


def _process_write_args(ds: Dataset):
    """Process dataset writer arguments.

    Currently `compression`, `columns` and `**write_args`
    """
    kwargs = {}
    if ds.compression:
        kwargs["compression"] = ds.compression

    if ds.columns:
        kwargs["columns"] = ds.compression

    kwargs.update(ds.args)
    kwargs.update(ds.write_args)
    return kwargs
