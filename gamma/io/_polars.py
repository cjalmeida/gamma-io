"""IO support for Polars."""


import polars as pl

from . import dispatch
from ._core import get_dataset
from ._fs import get_fs_path
from ._logging import log_ds_read, log_ds_write
from ._types import Dataset


@dispatch
def read_polars(
    layer: str, name: str, *, args=None, columns=None, **path_params
) -> pl.DataFrame:
    """Load a dataset as Polars Dataframe.

    See `get_dataset` for arguments.
    """
    ds = get_dataset(layer, name, args=args, columns=columns, **path_params)
    return read_polars(ds)


@dispatch
@log_ds_read
def read_polars(ds: Dataset):
    fs, path = get_fs_path(ds)

    match ds.format:
        case "parquet":
            kwargs = dict(use_pyarrow=True, pyarrow_options={"filesystem": fs})
            kwargs.update(ds.args)
            return pl.read_parquet(path, **kwargs)

        case _:
            raise ValueError(f"Reading Polars format not supported yet: {ds.format}")


@dispatch
def write_polars(df: pl.DataFrame, layer: str, name: str, *, args=None, **path_params):
    """Write a polars DataFrame to a dataset.

    See `get_dataset` for arguments.
    """
    ds = get_dataset(layer, name, args=args, **path_params)
    return write_polars(df, ds)


@dispatch
@log_ds_write
def write_polars(df: pl.DataFrame, ds: Dataset):
    """Write a polars DataFrame to a dataset."""
    fs, path = get_fs_path(ds)

    with log_ds_read(ds):
        match ds.format:
            case "parquet":
                kwargs = dict(use_pyarrow=True, pyarrow_options={"filesystem": fs})
                kwargs.update(ds.args)
                df.write_parquet(path, **kwargs)

            case _:
                msg = f"Writing Polars format not supported yet: {ds.format}"
                raise ValueError(msg)
