"""Module implementing most of staging functionality."""

from contextlib import contextmanager

from . import dispatch
from ._dataset import get_dataset_location
from ._fs import FSPathType, get_fs_path
from ._types import Dataset
from .config import get_staging_config

_FORCE_STAGING = False


def is_staging_enabled() -> bool:
    """Return if staging behavior is enabled.

    In interactive use (eg. Jupyter) you can use `use_staging` context manager to enable
    it and read datasets from staging areas.
    """
    if _FORCE_STAGING:
        return True

    return get_staging_config().use_staging


def get_staging_location() -> str:
    return get_staging_config().location


@contextmanager
def use_staging():
    global _FORCE_STAGING
    _FORCE_STAGING = True
    yield
    _FORCE_STAGING = False


@dispatch
def get_fs_path(ds: Dataset, staging: bool) -> FSPathType:
    """`get_fs_path` that can use staging locations."""
    loc = get_dataset_location(ds, staging)
    return get_fs_path(loc)


def get_stage_reader_fs_path(ds: Dataset) -> FSPathType:
    """`get_fs_path` for readers that takes into account the staging area.

    This will check the availability of the dataset in the staging area and return a
    `(fs, path)` tuple pointing to that. If not available, it will point to the original
    location.
    """
    if not is_staging_enabled():
        return get_fs_path(ds, False)

    # try staging, we just check for dataset existence for now
    fs, path = get_fs_path(ds, True)
    if fs.exists(path):
        return fs, path

    # get the original dataset
    return get_fs_path(ds, False)


def get_stage_writer_fs_path(ds: Dataset) -> FSPathType:
    """`get_fs_path` for writers that takes into account the staging area.

    This will always return the staging `(fs, path)` if staging is enable in config.
    """
    return get_fs_path(ds, is_staging_enabled())
