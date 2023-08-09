"""Main helper module for dealing directly with storage and filesystems."""

from pathlib import Path
from urllib.parse import urlsplit

import fsspec

from ._types import Dataset
from . import dispatch

from plum import Val


def get_fs_config(location: str) -> dict:
    from .config import get_filesystems_config

    u = urlsplit(location, "file")
    config = get_filesystems_config().get(u.scheme) or {"protocol": u.scheme}
    return u, config


@dispatch
def get_fs_path(proto, location) -> tuple[fsspec.AbstractFileSystem, str]:
    raise ValueError(f"Protocol not yet supported: {proto}, at location {location}")


@dispatch
def get_fs_path(ds: Dataset):
    from ._core import get_dataset_location

    return get_fs_path(get_dataset_location(ds))


@dispatch
def get_fs_path(location: str):
    # delegate to protocol specific implementation
    _, fsconfig = get_fs_config(location)
    proto = fsconfig["protocol"]
    return get_fs_path(Val[proto](), location)


@dispatch
def get_fs_path(proto: Val["file"], location: str):
    u, config = get_fs_config(location)
    path = Path(config.pop("path", "/"))
    lpath = path.absolute() / (u.hostname or "") / u.path.lstrip("/")
    return (fsspec.filesystem(**config), str(lpath))


@dispatch
def get_fs_path(proto: Val["https"], location: str):
    u, config = get_fs_config(location)
    return (fsspec.filesystem(**config), location)
