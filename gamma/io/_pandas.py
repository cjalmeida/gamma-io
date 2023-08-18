from typing import Literal

import pandas as pd

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
def read_pandas(*args, **kwargs) -> pd.DataFrame:
    """Pandas dataset reader shortcut."""
    return read_pandas(get_dataset(*args, **kwargs))


@dispatch
@log_ds_read
def read_pandas(ds: Dataset):
    """Pandas dataset reader shortcut."""
    # Delegate to pyarrow if supported
    if is_arrow_readable(ds):
        tb = read_arrow_table(ds)
        return tb.to_pandas()

    return read_pandas(ds, ds.format, ds.protocol)


@dispatch
def read_pandas(ds: Dataset, fmt, protocol):
    """Fallback reader for any format and storage protocol.

    We assume the storage to be `fsspec` stream compatible (ie. single file).
    """
    # get reader function based on format name
    func = getattr(pd, f"read_{fmt}", None)
    if func is None:
        ValueError(f"Reading Pandas format not supported yet: {fmt}")

    # get a fs, path reference
    fs, path = get_fs_path(ds)

    # process arguments
    kwargs = _process_read_args(ds)

    # stream and read data
    with fs.open(path, "rb") as fo:
        return func(fo, **kwargs)


def _process_read_args(ds: Dataset):
    """Process dataset reader arguments.

    Currently `compression`, `columns`, `**args`, `**read_args`
    """
    kwargs = {}
    if ds.compression:
        kwargs["compression"] = ds.compression

    if ds.columns:
        kwargs["columns"] = ds.compression

    kwargs.update(ds.args)
    kwargs.update(ds.read_args)
    return kwargs


@dispatch
def write_pandas(df: pd.DataFrame, *args, **kwargs) -> None:
    ds = get_dataset(*args, **kwargs)
    return write_pandas(df, ds)


@dispatch
@log_ds_write
def write_pandas(df: pd.DataFrame, ds: Dataset) -> None:
    """Write a polars DataFrame to a dataset."""
    from pyarrow import Table

    # Delegate to pyarrow if supported
    if is_arrow_writeable(ds):
        tb = Table.from_pandas(df)
        return write_arrow_table(tb, ds)

    return write_pandas(df, ds, ds.format, ds.protocol)


@dispatch
def write_pandas(df: pd.DataFrame, ds: Dataset, fmt, protocol):
    """We assume the storage to be `fsspec` stream compatible (ie. single file)."""
    # get reader function based on format name
    func = getattr(pd.DataFrame, f"to_{fmt}", None)
    if func is None:
        ValueError(f"Writing Pandas format not supported yet: {fmt}")

    # get a fs, path reference
    fs, path = get_fs_path(ds)

    # process arguments
    kwargs = process_write_args(ds, fmt)

    # stream and read data
    with fs.open(path, "wb") as fo:
        return func(df, fo, **kwargs)


@dispatch
def process_write_args(ds: Dataset, fmt):
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


@dispatch
def process_write_args(ds: Dataset, fmt: Literal["csv"]):
    """Process dataset writer arguments for CSVs.

    Currently `compression`, `columns` and `**write_args`
    """
    kwargs = dict(index=False, compression=ds.compression, columns=ds.columns)
    kwargs.update(ds.args)
    kwargs.update(ds.write_args)
    return kwargs
