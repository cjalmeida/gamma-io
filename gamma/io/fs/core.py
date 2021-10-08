from functools import lru_cache
from typing import Tuple, Union
from fsspec import AbstractFileSystem
import fsspec
from gamma.dispatch import dispatch, Val
from .config import (
    FileScheme,
    FileSystems,
    LocalFS,
    S3FS,
    S3Scheme,
    S3_HTTPS_FS,
    S3_HTTP_FS,
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


@dispatch
def get_fs_path(
    uri: str, cfg: Union[S3_HTTP_FS, S3_HTTPS_FS]
) -> Tuple[AbstractFileSystem, str]:

    if isinstance(cfg, S3_HTTP_FS):
        endpoint_scheme = "http"
    elif isinstance(cfg, S3_HTTPS_FS):
        endpoint_scheme = "https"
    else:
        raise ValueError(cfg)

    options = get_options(cfg)
    endpoint_url = f"{endpoint_scheme}://" + options["host"]
    options["client_kwargs"] = dict(endpoint_url=endpoint_url)

    del options["host"]
    del options["scheme"]
    del options["bucket"]

    u = split_url(uri)

    fs = fsspec.filesystem(protocol="s3", **options)
    path = (u.path or "").strip("/")
    return fs, path

    return get_fs_path[str, S3FS](uri, cfg, options=options)


@dispatch
def get_fs_path(
    uri: str, cfg: S3FS, *, options: dict = None
) -> Tuple[AbstractFileSystem, str]:

    options = options or get_options(cfg)
    options.pop("scheme", None)
    options.pop("bucket", None)

    u = split_url(uri)
    fs = fsspec.filesystem(protocol="s3", **options)
    path = u.netloc + "/" + (u.path or "").strip("/")
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


@dispatch
def match_score(cfg: S3FS, uri: str) -> int:
    u = split_url(uri)
    bucket = u.hostname
    return int(cfg.bucket == bucket)


@dispatch
def match_score(cfg: Union[S3_HTTPS_FS, S3_HTTP_FS], uri: str) -> int:
    u = split_url(uri)
    host = u.netloc
    bucket, *_ = u.path.lstrip("/").split("/", 1)
    if host != cfg.host:
        return 0
    return 1 + int(cfg.bucket == bucket)


@lru_cache(10)
def split_url(uri) -> urllib.parse.SplitResult:
    return urllib.parse.urlsplit(uri)
