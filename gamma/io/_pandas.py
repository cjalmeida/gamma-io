from typing import Literal

import pandas as pd

from . import dispatch
from ._arrow import process_arrow_read_args, process_arrow_write_args
from ._dataset import get_dataset
from ._fs import get_fs_path
from ._logging import log_ds_read, log_ds_write
from ._types import FEATHER_STRINGS, Dataset, PyArrowFmt
from ._utils import func_arguments


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
    # get reader function based on format name
    func = getattr(pd, f"read_{fmt}", None)
    if func is None:
        NotImplemented(f"Format not supported {fmt}")

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
def read_pandas(ds: Dataset, fmt: PyArrowFmt, protocol):
    """Specialized support for PyArrow datasets."""
    # get a fs, path reference
    fs, path = get_fs_path(ds)

    # process arguments
    kwargs = process_arrow_read_args(ds)
    kwargs["engine"] = "pyarrow"
    kwargs["filesystem"] = fs
    kwargs["path"] = path

    # stream and read data
    match fmt:
        case "parquet":
            return pd.read_parquet(**kwargs)
        case f if f in FEATHER_STRINGS:
            return pd.read_feather(**kwargs)
        case _:
            raise NotImplementedError(f"Format not supported {fmt}")


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
    fargs = func_arguments(func)

    # get a fs, path reference
    fs, path = get_fs_path(ds)

    # process arguments
    kwargs = process_write_args(ds, fmt, fargs)

    # stream and read data
    with fs.open(path, "wb") as fo:
        return func(df, fo, **kwargs)


@dispatch
def write_pandas(df: pd.DataFrame, ds: Dataset, fmt: PyArrowFmt, protocol) -> None:
    # get a fs, path reference
    fs, path = get_fs_path(ds)

    # process arguments
    kwargs = process_arrow_write_args(ds)
    kwargs["index"] = False
    kwargs["engine"] = "pyarrow"
    kwargs["path"] = path
    kwargs["filesystem"] = fs

    match fmt:
        case "parquet":
            df.to_parquet(**kwargs)
        case f if f in FEATHER_STRINGS:
            df.to_feather(**kwargs)
        case _:
            raise NotImplementedError(f"Format not supported {fmt}")


@dispatch
def process_write_args(ds: Dataset, fmt, fargs):
    """Process dataset writer arguments.

    Currently `compression`, `columns` and `**write_args`
    """
    kwargs = {}
    if "compression" in fargs and ds.compression is not None:
        kwargs["compression"] = ds.compression

    if "columsn in fargs" and ds.columns:
        kwargs["columns"] = ds.compression

    kwargs.update(ds.args)
    kwargs.update(ds.write_args)
    return kwargs


@dispatch
def process_write_args(ds: Dataset, fmt: Literal["csv"], fargs):
    """Process dataset writer arguments for CSVs.

    Currently `compression`, `columns` and `**write_args`
    """
    kwargs = dict(index=False, compression=ds.compression, columns=ds.columns)
    kwargs.update(ds.args)
    kwargs.update(ds.write_args)
    return kwargs


@dispatch
def list_partitions(*args, **kwargs) -> pd.DataFrame:
    """List the existing partition set.

    Return a Dataframe with the available partitions and size.
    """
    ds = get_dataset(*args, **kwargs)
    ds.columns = ds.partition_by

    return read_pandas(ds).drop_duplicates()
