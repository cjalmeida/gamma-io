"""Module for dealing with filesystems-like storage.

We rely heavily on `fsspec` for most operation. The main function `get_fs_path`" will
return a `(fs: FileSystem, path: str)` tuple.
"""

import re
from pathlib import Path
from typing import Literal
from urllib.parse import SplitResult, urlsplit

import fsspec

from . import dispatch
from ._types import Dataset
from .config import get_filesystems_config

FSPathType = tuple[fsspec.AbstractFileSystem, str]


def get_fs_options(location: str) -> tuple[SplitResult, dict]:
    """Return the `fsspec` storage options to construct a `FileSystem` object.

    We check the `filesystems` configuration for "match" keys providing a regex
    pattern we run against the location URL. We then pick the first matching entry
    as our storage options.

    If no entries match, we return a dummy `{'protocol': scheme}` where `scheme` is the
    "URL scheme" part of the location.

    Notes:
        - When creating `FileSystem` objects, you need to "pop" out the `protocol` key
          from the options dictionary.

        - The options dict always have the `protocol` entry, defaulting to the
          URL scheme of `location`.
    """
    u = urlsplit(location, "file")
    filesystems = get_filesystems_config()

    # try to find a matching entry
    options = None
    for candidate in filesystems.values():
        pattern = candidate["match"]
        if re.match(pattern, location):
            options = candidate.copy()
            break

    # no entry found, fallback to simple protocol
    if options is None:
        return u, {"protocol": u.scheme}

    if "protocol" not in options:
        options["protocol"] = u.scheme

    options.pop("match")

    return u, options


@dispatch
def get_fs_path(proto, location) -> FSPathType:
    """Fallback when a protocol has no specialization."""
    _, options = get_fs_options(location)
    protocol = options.pop("protocol")

    fs, _, (path,) = fsspec.get_fs_token_paths(
        location, protocol=protocol, storage_options=options
    )

    return fs, path


@dispatch
def get_fs_path(ds: Dataset) -> FSPathType:
    from ._dataset import get_dataset_location

    return get_fs_path(get_dataset_location(ds))


@dispatch
def get_fs_path(location: str):
    # delegate to protocol specific implementation
    _, fsconfig = get_fs_options(location)
    proto = fsconfig["protocol"]
    return get_fs_path(proto, location)


@dispatch
def get_fs_path(proto: Literal["file"], location: str):
    u, config = get_fs_options(location)
    path = Path(config.pop("path", "/"))
    lpath = path.absolute() / (u.hostname or "") / u.path.lstrip("/")
    return (fsspec.filesystem(**config), str(lpath))


@dispatch
def get_fs_path(proto: Literal["https"], location: str):
    u, config = get_fs_options(location)
    return (fsspec.filesystem(**config), location)
