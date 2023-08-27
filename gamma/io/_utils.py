"""Helper module to deal with dynamic imports."""

import importlib
import inspect
import os.path
import sys

from fsspec import AbstractFileSystem


def try_import(module_name: str):
    try:
        return importlib.import_module(module_name)
    except ModuleNotFoundError:  # pragma: no cover
        return None


def func_arguments(f) -> list[str]:
    spec = inspect.getfullargspec(f)
    return set(spec.args + spec.kwonlyargs)


def remove_extra_arguments(f, kwargs: dict) -> None:
    """Inplace remove extra keys from `kwargs` for a given function."""
    fargs = func_arguments(f)
    for name in list(kwargs):
        if name not in fargs:
            del kwargs[name]


def progress(*, total: int, force_tty=False):
    """Initialize a progress bar.

    Supports tqdm.

    Returns: (update, close) functions
    """
    if not force_tty and not sys.stdout.isatty():
        return lambda: None, lambda: None

    if try_import("tqdm"):
        from tqdm import tqdm

        bar = tqdm(total=total)
        return bar.update, bar.close

    # not installed, return no-op
    return lambda: None, lambda: None


def get_parent(path: str) -> str:
    """Return the parent of the path."""
    return os.path.dirname(path.rstrip("/"))


def get_single_file_in_folder(fs: AbstractFileSystem, path, ds=None):
    from ._dataset import get_dataset_location
    from ._types import DatasetException

    def _raise(msg):  # helper for raising better exceptions
        if ds:
            loc = get_dataset_location(ds)
            fullmsg = f"Dataset {ds.layer}/{ds.name}: location '{loc}' {msg}"
            raise DatasetException(fullmsg, ds)
        else:
            fullmsg = f"(proto={fs.protocol}, path={loc}) {msg}"
            raise DatasetException(fullmsg, ds)

    if not fs.exists(path):
        _raise("does not exist.")

    files = fs.find(path)

    if len(files) == 0:
        _raise("refers to an empty directory.")

    if len(files) > 1:
        _raise(" contains many files and reader can only read a single file.")

    path = files[0]
    return path
