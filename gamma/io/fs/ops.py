"""Functional multi-dispatch enabled operations for filesystems"""

from fsspec import AbstractFileSystem
from gamma.dispatch import dispatch


@dispatch
def ls(fs: AbstractFileSystem, path: str, **kwargs):
    return fs.ls(path, **kwargs)


@dispatch
def open(fs: AbstractFileSystem, path: str, mode: str = "rb", **kwargs):
    return fs.open(path, mode, **kwargs)


@dispatch
def parent_path(path: str) -> str:
    if path == "/":
        return "/"

    parent, _ = path.rstrip("/").rsplit("/", 1)
    return parent


@dispatch
def makedirs(fs: AbstractFileSystem, path: str, *, exist_ok: bool = True) -> None:
    fs.makedirs(path, exist_ok=exist_ok)


@dispatch
def cat_file(fs: AbstractFileSystem, path: str) -> bytes:
    return fs.cat_file(path)


@dispatch
def put_file(fs: AbstractFileSystem, lpath: str, rpath: str) -> None:
    fs.put_file(lpath, rpath)


@dispatch
def get_file(fs: AbstractFileSystem, rpath: str, lpath: str) -> None:
    fs.get_file(rpath, lpath)
