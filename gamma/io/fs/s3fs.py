"""Support for S3 (and compatible), require the installation of the `s3fs` library."""

from typing import Tuple
from fsspec import AbstractFileSystem
import fsspec
from gamma.dispatch import dispatch
from .config import (
    S3FS,
)
from .core import match_score, get_fs_path, split_url


@dispatch
def match_score(cfg: S3FS, uri: str) -> int:
    """Match config entry against URI `netloc`.

    For `s3` URIs, the format should be `s3://{bucket}/{path}` for AWS S3 buckets
    or `s3://{endpoint_url.netloc}/{bucket}/{path}` for S3 compatible implementations.

    When matching against S3-compatible services, matching `bucket` is optional but
    a strict bucket match has perference
    """
    u = split_url(uri)
    cfg_netloc = split_url(cfg.endpoint_url).netloc if cfg.endpoint_url else cfg.bucket
    uri_bucket = u.path.strip("/").split("/", 1)[0] if cfg.endpoint_url else cfg.bucket
    return int(cfg_netloc == u.netloc) + int(cfg.bucket == uri_bucket)


@dispatch
def get_fs_path(
    uri: str, cfg: S3FS, *, options: dict = None
) -> Tuple[AbstractFileSystem, str]:

    options = options or cfg.dict()
    options.pop("uri", None)
    options.pop("scheme", None)
    options.pop("bucket", None)
    endpoint_url = options.pop("endpoint_url", None)
    if endpoint_url:
        options["client_kwargs"] = dict(endpoint_url=endpoint_url)

    u = split_url(uri)
    fs = fsspec.filesystem(protocol="s3", **options)

    if endpoint_url:
        path = u.path or ""
    else:
        path = u.netloc + "/" + (u.path or "")

    path = path.strip("/")
    return fs, path
