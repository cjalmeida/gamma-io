import pandas as pd
from . import dispatch

from ._core import get_dataset
from ._fs import get_fs_path
from ._types import Dataset


@dispatch
def read_pandas(
    layer: str, name: str, *, args=None, columns=None, **path_params
) -> pd.DataFrame:
    ds = get_dataset(layer, name, args=args, columns=columns, **path_params)
    return read_pandas(ds)


@dispatch
def read_pandas(ds: Dataset):
    fs, path = get_fs_path(ds)

    match ds.format:
        case "parquet":
            kwargs = dict(engine="pyarrow", filesystem=fs)
            kwargs.update(ds.args)
            return pd.read_parquet(path, **kwargs)

        case _:
            raise ValueError(f"Reading Pandas format not supported yet: {ds.format}")


@dispatch
def write_pandas(df: pd.DataFrame, layer: str, name: str, *, args=None, **path_params):
    ds = get_dataset(layer, name, args=args, **path_params)
    return write_pandas(ds)


@dispatch
def write_pandas(df: pd.DataFrame, ds: Dataset):
    """Write a polars DataFrame to a dataset."""
    fs, path = get_fs_path(ds)

    match ds.format:
        case "parquet":
            kwargs = dict(engine="pyarrow", filesystem=fs, compression="zstd")
            kwargs.update(ds.args)
            df.to_parquet(path, **kwargs)

        case _:
            msg = f"Writing Pandas format not supported yet: {ds.format}"
            raise ValueError(msg)
