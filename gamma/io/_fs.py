"""Main helper module for dealing directly with storage and filesystems."""

from pathlib import Path
from urllib.parse import urlsplit

import fsspec

from ._types import Dataset


def get_fs_config(location: str) -> dict:
    from .config import get_filesystems_config

    u = urlsplit(location, "file")
    config = get_filesystems_config().get(u.scheme) or {"protocol": u.scheme}
    return u, config


def get_fs_path(*args) -> tuple[fsspec.AbstractFileSystem, str]:
    """Return a `(filesystem, path)` tuple to be used with `fsspec` library.

    The actual implementation is protocol specific, so you need to refer to
    `_get_fs_path_{proto}` methods.

    Positional Arguments:
        Dataset: a Dataset object
        location (str): a location URL
        proto (str), location (str): a protocol specific location URL

    """
    # avoid circular import
    from ._core import get_dataset_location

    match args:
        case [Dataset() as ds]:
            return get_fs_path(get_dataset_location(ds))

        case [str() as location]:
            # delegate to protocol specific implementation
            _, fsconfig = get_fs_config(location)
            proto = fsconfig["protocol"]
            return get_fs_path(proto, location)

        case [proto, location]:
            # delegate to protocol specific implementation
            try:
                func = globals()[f"_get_fs_path_{proto}"]
            except KeyError:
                raise ValueError(
                    f"Protocol not yet supported: {proto}, at location {location}"
                )

            return func(location)

        case _:
            raise ValueError(f"Can't get fs/path for args: {args}")


def _get_fs_path_file(location: str):
    u, config = get_fs_config(location)
    path = Path(config.pop("path", "/"))
    lpath = path.absolute() / (u.hostname or "") / u.path.lstrip("/")
    return (fsspec.filesystem(**config), str(lpath))


def _get_fs_path_https(location: str):
    u, config = get_fs_config(location)
    return (fsspec.filesystem(**config), location)
