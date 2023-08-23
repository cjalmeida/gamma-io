"""Helper module to deal with dynamic imports."""

import importlib
import inspect


def try_import(module_name: str):
    try:
        return importlib.import_module(module_name)
    except ModuleNotFoundError:  # pragma: no cover
        return None


def func_arguments(f) -> list[str]:
    spec = inspect.getfullargspec(f)
    return set(spec.args + spec.kwonlyargs)
