"""Helper module to deal with dynamic imports."""

import importlib
import inspect
import sys


def try_import(module_name: str):
    try:
        return importlib.import_module(module_name)
    except ModuleNotFoundError:  # pragma: no cover
        return None


def func_arguments(f) -> list[str]:
    spec = inspect.getfullargspec(f)
    return set(spec.args + spec.kwonlyargs)


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
