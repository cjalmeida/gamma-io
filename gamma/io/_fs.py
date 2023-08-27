"""Module for dealing with filesystems-like storage.

We rely heavily on `fsspec` for most operation. The main function `get_fs_path`" will
return a `(fs: FileSystem, path: str)` tuple.
"""

import re
import shutil
from typing import Literal
from urllib.parse import SplitResult, urlsplit

import fsspec
import fsspec.core

from . import dispatch
from ._types import Dataset, DatasetException
from ._utils import get_parent, progress
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
def get_fs_path(proto, location: str) -> FSPathType:
    """Fallback when a protocol has no specialization."""
    _, options = get_fs_options(location)
    options.pop("protocol")

    fs, path = fsspec.core.url_to_fs(location, **options)

    # keep promise of ending directories with a trailing slash as they're sometimes
    # removed by the url_to_fs call

    if location.endswith("/") and not path.endswith("/"):
        path = path.rstrip("/") + "/"

    return fs, path


@dispatch
def get_fs_path(ds: Dataset) -> FSPathType:
    from ._dataset import get_dataset_location

    loc = get_dataset_location(ds)
    return get_fs_path(loc)


@dispatch
def get_fs_path(location: str):
    # delegate to protocol specific implementation
    _, fsconfig = get_fs_options(location)
    proto = fsconfig["protocol"]
    return get_fs_path(proto, location)


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
        dst_fs.makedirs(get_parent(dst_file), exist_ok=True)
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


@dispatch
def treat_loc_as_folder(ds: Dataset) -> bool:
    """Return if the dataset location should be treated as a folder.

    See `ds.is_file` for the heuristic behavior.
    """
    from ._dataset import get_dataset_location

    return treat_loc_as_folder(ds, get_dataset_location(ds))


def treat_loc_as_folder(ds: Dataset, path: str) -> bool:
    is_file = ds.is_file
    if is_file is None:
        _, last_part = path.rsplit("/", 1)

        # use heuristic
        if ds.partition_by:
            is_file = False
        elif path.endswith("/"):
            is_file = False
        elif "." in last_part:
            is_file = True
        else:
            is_file = False

    if is_file and ds.partition_by:
        msg = f"Partitioned dataset {ds} cannot have `is_file == True`."
        raise DatasetException(msg, ds)

    return not is_file
