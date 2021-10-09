"""Functional multi-dispatch enabled operations for filesystems.

These ops are designed to complement `fsspec` and provide cross filesystem optimized
operations.
"""

from gamma.dispatch import dispatch


@dispatch
def parent_path(path: str) -> str:
    """Get the parent directory of a path."""

    if path == "/":
        return "/"

    parent, _ = path.rstrip("/").rsplit("/", 1)
    return parent
