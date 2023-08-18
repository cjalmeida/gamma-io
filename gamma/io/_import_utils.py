"""Helper module to deal with dynamic imports."""

import importlib


def try_import(module_name: str):
    try:
        return importlib.import_module(module_name)
    except ModuleNotFoundError:  # pragma: no cover
        return None
