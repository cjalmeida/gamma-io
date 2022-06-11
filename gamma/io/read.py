"""Basic functions for reading data provided by a dataset."""

from fsspec import AbstractFileSystem
from gamma.io.catalog import URI, Dataset, get_dataset_entry
from gamma.io.types import BytesOutputType, OutputType, get_output_type
from gamma.dispatch import dispatch
from gamma.io.fs import get_fs_path


@dispatch
def read_data(out_class, ds_group: str, ds_name: str):
    """Generic function to read data into an output type.

    We call `get_output_type` to find the actual OutputType
    """
    _type = get_output_type(out_class)
    return read_data(_type, ds_group, ds_name)


@dispatch
def get_fs_path(ds: Dataset):
    return get_fs_path(ds.location)


@dispatch
def get_fs_path(uri: URI):
    return get_fs_path(uri.uri)


@dispatch
def read_data(_: BytesOutputType, ds_group: str, ds_name: str) -> bytes:
    ds = get_dataset_entry(ds_group, ds_name)
    fs: AbstractFileSystem
    path: str
    fs, path = get_fs_path(ds)
    fs.cat_file(path)


def read_bytes(ds_group: str, ds_name: str):
    """Shortcut to read_data(bytes, ...)"""
    return read_data(bytes, ds_group, ds_name)
