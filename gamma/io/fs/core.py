from functools import lru_cache
from typing import Tuple, Union
from fsspec import AbstractFileSystem
import fsspec
from gamma.dispatch import dispatch, Val
from .config import (
    FileSystems,
    LocalFS,
    S3FS,
    get_filesystems,
    FSConfig,
)
import urllib.parse


@dispatch
def get_fs_path(uri: str, filesystems) -> Tuple[AbstractFileSystem, str]:
    """Split a FSSpec compatible URI into a `fsspec.AbstractFileSystem` and path
    string"""

    fs_cfg = match_uri(uri, filesystems)
    return get_fs_path(uri, fs_cfg)


@dispatch
def get_fs_path(uri: str) -> Tuple[AbstractFileSystem, str]:
    return get_fs_path(uri, get_filesystems())


@dispatch
def get_fs_path(uri: str, cfg: LocalFS) -> Tuple[AbstractFileSystem, str]:
    """Get fsspec FileSystem and path from a `file:` URLs"""

    if not uri.startswith("file:///"):
        raise ValueError(f"'file:' URI must start with 'file:///` but we got {uri}")

    u = split_url(uri)
    fs = fsspec.filesystem(protocol="file")
    path = u.path.rstrip("/")
    return fs, path


def get_options(cfg: FSConfig):
    return cfg.dict()


@dispatch
def match_uri(uri: str, filesystems: FileSystems) -> FSConfig:
    """Match a URI to a filesystem config.

    Raise `ValueError` if no fs config matches.
    """

    u = split_url(uri)
    scheme: Val = Val[u.scheme]

    matches = []
    cfg: FSConfig
    for _, cfg in filesystems:
        if scheme.value != cfg.scheme:
            continue
        score = match_score(cfg, uri)
        if score > 0:
            matches.append((score, cfg))

    if not matches:
        raise ValueError(f"No filesystem configuration for URI: {uri}")

    matches.sort()
    return matches[-1][1]


@dispatch
def match_score(cfg, uri) -> int:
    # fallback
    return 0


@dispatch
def match_score(cfg: LocalFS, uri: str) -> int:
    # no extra check required
    return 1


@lru_cache(100)
def split_url(uri) -> urllib.parse.SplitResult:
    return urllib.parse.urlsplit(uri)
