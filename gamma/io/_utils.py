"""Helper module to deal with dynamic imports."""

import importlib
import inspect
import os.path
import sys


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
