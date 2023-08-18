"""Module implementing SQL read/write via Pandas and SQLAlchemy.

This module takes care of initializing and maintaining DB connections. **It only
supports a single process and thread**. So, in a multiprocessing/multithreading setting,
take care to read/write only from the main process/thread.
"""

import hashlib
import pickle
from typing import Any, Literal
from urllib.parse import urlparse

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from . import dispatch
from ._dataset import get_dataset_location
from ._fs import get_fs_path
from ._types import Dataset, DatasetException

# Global engine cache, should be created one per process/thread.
_ENGINE_CACHE: dict[str, Engine] = dict()

# Flags to allow doing stuff outside the main thread/process.

_ALLOW_NON_MAIN_THREAD: bool = False
_ALLOW_NON_MAIN_PROCESS: bool = False


def set_allow_sql_from_non_main_thread(v: bool):
    """Set this to True to allow SQL operations not happening from the main thread.

    I hope you know what you're doing.
    """
    global _ALLOW_NON_MAIN_THREAD
    _ALLOW_NON_MAIN_THREAD = True


def set_allow_sql_from_non_main_process(v: bool):
    """Set this to True to allow SQL operations not happening from the main process.

    I hope you know what you're doing.
    """
    global _ALLOW_NON_MAIN_PROCESS
    _ALLOW_NON_MAIN_PROCESS = True


@dispatch
def read_pandas(ds: Dataset, fmt: Literal["sql_table"], protocol: Literal["sql"]):
    """Support for reading a from a SQL database table via SQLAlchemy."""
    # get a sqlalch engine
    engine = get_sql_engine(ds)

    kwargs = {"con": engine}
    kwargs.update(ds.args)
    kwargs.update(ds.read_args)
    kwargs["table_name"] = kwargs.pop("table_name", None) or kwargs.pop("name", None)

    if kwargs["table_name"] is None:  # pragma: no cover
        msg = "Missing 'args.name' or 'args.table_name' from the dataset specification."
        raise DatasetException(msg, ds)

    return pd.read_sql_table(**kwargs)


@dispatch
def read_pandas(ds: Dataset, fmt: Literal["sql_query"], protocol: Literal["sql"]):
    """Support for loading a dataset by running a SQL query via SQLAlchemy.

    The SQL query can be parameterized and values pushed using `Dataset.params`
    """
    # get a sqlalch engine
    engine = get_sql_engine(ds)

    kwargs = {"con": engine, "params": ds.params}
    kwargs.update(ds.args)
    kwargs.update(ds.read_args)

    sql = kwargs.pop("sql", None)

    if sql is None:  # pragma: no cover
        msg = (
            "Missing 'args.sql' from the dataset specification. You must provide a "
            "textual SQL query or a URL to a file"
        )
        raise DatasetException(msg, ds)

    kwargs["sql"] = load_sql_statement(sql)

    return pd.read_sql_query(**kwargs)


def load_sql_statement(sql: str) -> str:
    """Return a SQL query from provided `sql` arg.

    We detect if sql is an URL, and if so we get the contents treating it as a
    'location' string, otherwise we return the string as is.
    """
    if urlparse(sql).scheme:
        fs, path = get_fs_path(sql)
        return fs.read_text(path)

    return sql


@dispatch
def write_pandas(df: pd.DataFrame, ds: Dataset, fmt, protocol: Literal["sql"]) -> None:
    """Support for writing a full table into a SQL database via SQLAlchemy.

    This defaults to:
        - replacing the table if exists (be careful!)
        - not writing indexes
        - using 'multi' method of insertion
    """
    # get a sqlalch engine
    engine = get_sql_engine(ds)

    kwargs = {"con": engine, "if_exists": "replace", "index": False, "method": "multi"}
    kwargs.update(ds.args)
    kwargs.update(ds.write_args)

    kwargs["name"] = kwargs.pop("name", None) or kwargs.pop("table_name", None)
    kwargs["method"] = get_sql_insert_method(kwargs["method"])

    if kwargs["name"] is None:  # pragma: no cover
        msg = "Missing 'args.name' or 'args.table_name' from the dataset specification."
        raise DatasetException(msg, ds)

    df.to_sql(**kwargs)


@dispatch
def get_sql_insert_method(method: str | None) -> Any:
    """Overload this function to return your own insert handler."""
    return method


def get_sql_engine(ds: Dataset) -> Engine:
    """Return a SQLAlchemy Engine able to connect to the dataset."""
    # split SQLAlchemy URL from  ds
    location = get_dataset_location(ds)
    _, sqla_url = location.split(":", 1)

    # create the engine
    engine = _get_or_create_engine(sqla_url, **ds.engine_args)
    return engine


def _validate_sql_access():
    """Ensure proper process/thread when accessing the engine."""
    import multiprocessing as mp
    import threading

    if not _ALLOW_NON_MAIN_THREAD:
        assert (
            threading.current_thread() is threading.main_thread()
        ), "SQL operations can only happen from the main thread."

    if not _ALLOW_NON_MAIN_PROCESS:
        assert (
            mp.current_process().name == "MainProcess"
        ), "SQL operations can only happen from the main process."


def _get_or_create_engine(url: str, **kwargs) -> Engine:
    """Get or create an Engine from the engine cache."""
    _validate_sql_access()

    # convert kwargs to sorted tuple sequence
    args_list = []
    for k, v in kwargs.items():
        # ignore unhashable params
        try:
            hash(v)
        except TypeError:  # pragma: no cover
            continue
        args_list.append((k, v))

    # hash args and url
    args_bytes = pickle.dumps(tuple(sorted(args_list)))
    args_hash: hashlib._Hash = hashlib.sha1(args_bytes, usedforsecurity=False)
    args_hash.update(url.encode())
    key = args_hash.digest()

    # get or create
    engine = _ENGINE_CACHE.get(key)
    if engine is None:
        engine = create_engine(url, **kwargs)
        _ENGINE_CACHE[key] = engine

    return engine
