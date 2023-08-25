"""Module for dealing with filesystems-like storage.

We rely heavily on `fsspec` for most operation. The main function `get_fs_path`" will
return a `(fs: FileSystem, path: str)` tuple.
"""

import re
import shutil
from pathlib import Path
from typing import Literal
from urllib.parse import SplitResult, urlsplit

import fsspec

from . import dispatch
from ._types import Dataset
from ._utils import progress
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
    config.setdefault("auto_mkdir", True)

    path = Path(config.pop("path", "/"))
    lpath = path.absolute() / (u.hostname or "") / u.path.lstrip("/")
    return (fsspec.filesystem(**config), str(lpath))


@dispatch
def get_fs_path(proto: Literal["https"], location: str):
    _, config = get_fs_options(location)
    return (fsspec.filesystem(**config), location)


@dispatch
def copy_dataset(src: Dataset, dst: Dataset) -> None:
    """Copy one dataset into another.

    The operation copy the the full dataset contents, replacing the target if it exists.

    Args:
        src: The source dataset
        dst: The destination dataset

    TODO: implement partitioned copy.
    """
    # for us to able to use direct file copy, we need to ensure datasets are the same
    # except for the layer/name
    exclude_attrs = ["layer", "name", "location", "params"]
    src_attrs = src.model_dump(exclude=exclude_attrs)
    dst_attrs = src.model_dump(exclude=exclude_attrs)

    if src_attrs != dst_attrs:
        raise ValueError(
            f"The src={src.layer}.{src.name} and dst={dst.layer}.{dst.name} datasets "
            "are not equivalent and cannot be directly copied."
        )

    elif src.protocol == dst.protocol:
        # try to use same-protocol optimized copy
        copy_dataset(src, dst, src.protocol)
    else:
        # fallback to generic copy
        copy_dataset(src, dst, None)


@dispatch
def copy_dataset(src: Dataset, dst: Dataset, proto):
    """Fallback copy operation, usually between diferent filesystems."""
    src_fs: fsspec.AbstractFileSystem
    dst_fs: fsspec.AbstractFileSystem
    src_fs, src_path = get_fs_path(src)
    dst_fs, dst_path = get_fs_path(dst)

    # ensure dst is empty
    if dst_fs.exists(dst_path):
        dst_fs.rm(dst_path, recursive=True)

    def strip_base(base, f):
        return f[len(base) :]

    # walk src and copy to dst
    files = src_fs.find(src_path)
    bar_update, bar_close = progress(total=len(files))
    for src_file in files:
        dst_file = dst_path + strip_base(src_path, src_file)
        with (
            src_fs.open(src_file, "rb") as src_fo,
            dst_fs.open(dst_file, "wb") as dst_fo,
        ):
            shutil.copyfileobj(src_fo, dst_fo)

        bar_update()

    bar_close()


@dispatch
def copy_dataset(src: Dataset, dst: Dataset, proto: Literal["s3"]):
    """Fallback copy operation, usually between diferent filesystems."""
    import s3fs

    src_fs: s3fs.S3FileSystem
    dst_fs: s3fs.S3FileSystem

    # test if all on same system
    src_fs, src_path = get_fs_path(src)
    dst_fs, dst_path = get_fs_path(dst)

    if not (
        src_fs.storage_options == dst_fs.storage_options
        and src_fs.client_kwargs == dst_fs.client_kwargs
    ):
        copy_dataset(src, dst, None)  # fallback

    # ensure dst is empty
    if dst_fs.exists(dst_path):
        dst_fs.rm(dst_path, recursive=True)

    # use fast copy
    src_fs.copy(src_path, dst_path, recursive=True)
