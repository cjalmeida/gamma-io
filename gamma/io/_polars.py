"""IO support for Polars."""


import shutil
from io import BytesIO

import polars as pl

from . import dispatch
from ._arrow import process_arrow_read_args, process_arrow_write_args
from ._dataset import get_dataset
from ._fs import get_fs_path
from ._logging import log_ds_read, log_ds_write
from ._types import FEATHER_STRINGS, Dataset, PyArrowFmt
from ._utils import func_arguments


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
    fs, path = get_fs_path(ds)

    # process arguments
    kwargs = dict()
    kwargs.update(ds.args)
    kwargs.update(ds.read_args)

    fargs = func_arguments(func)

    if "compression" in fargs:
        kwargs["compression"] = ds.compression

    if "columns" in fargs:
        kwargs["columns"] = ds.columns

    # stream and read data
    with fs.open(path, "rb") as fo:
        return func(fo, **kwargs)


@dispatch
def read_polars(ds: Dataset, fmt: PyArrowFmt, protocol):
    """Read PyArrow compatible formats into a DataFrame."""
    fs, path = get_fs_path(ds)
    arrow_args = process_arrow_read_args(ds)
    arrow_args["filesystem"] = fs

    match fmt:
        case "parquet":
            return pl.read_parquet(
                source=path, use_pyarrow=True, pyarrow_options=arrow_args
            )
        case f if f in FEATHER_STRINGS:
            with fs.open(path, "rb") as fo:
                return pl.read_ipc(source=fo, use_pyarrow=True)
        case _:
            raise NotImplementedError(f"Format not supported {fmt}")


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

    fargs = func_arguments(func)

    # get a fs, path reference
    fs, path = get_fs_path(ds)

    # process arguments
    _kwargs = dict(compression=ds.compression, columns=ds.columns)
    _kwargs.update(ds.args)
    _kwargs.update(ds.write_args)
    kwargs = {k: v for k, v in _kwargs.items() if k in fargs}

    # stream and read data
    with fs.open(path, "wb") as fo:
        return func(df, fo, **kwargs)


@dispatch
def write_polars(df: pl.DataFrame, ds: Dataset, fmt: PyArrowFmt, protocol) -> None:
    """Write DataFrames formats into a PyArrow compatible dataset."""
    fs, path = get_fs_path(ds)

    match fmt:
        case "parquet":
            arrow_args = process_arrow_write_args(ds)
            arrow_args["filesystem"] = fs
            df.write_parquet(file=path, use_pyarrow=True, pyarrow_options=arrow_args)

        case f if f in FEATHER_STRINGS:
            kwargs = dict(compression=ds.compression or "uncompressed")

            if fs.protocol == "file":
                df.write_ipc(path, **kwargs)

            else:
                data: BytesIO = df.write_ipc(None, **kwargs)
                with fs.open(path, "wb") as dst_fo:
                    shutil.copyfileobj(data, dst_fo)

        case _:
            raise NotImplementedError(f"Format not supported {fmt}")
