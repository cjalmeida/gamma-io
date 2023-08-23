"""Logging utilities for io."""

import logging
import time

from decorator import decorator

from ._dataset import get_dataset_location
from ._types import Dataset

logger = logging.getLogger("gamma.io")


def _pre_log(msg):
    logger.info(msg)
    _start = time.monotonic()
    return _start


def _post_log(_start):
    msg = f"Took {(time.monotonic() - _start):.2f}s"
    logger.info(msg)


@decorator
def log_ds_read(func, *args):
    ds = args[0]

    """Decorator to track dataset reads."""
    start = _pre_log(
        f"Reading dataset {ds.layer}.{ds.name} at '{get_dataset_location(ds)}'"
    )
    ret = func(ds)
    _post_log(start)
    return ret


@decorator
def log_ds_write(func, data, ds: Dataset):
    """Decorator to track dataset writes."""
    start = _pre_log(
        f"Writing dataset {ds.layer}.{ds.name} to '{get_dataset_location(ds)}'"
    )
    ret = func(data, ds)
    _post_log(start)
    return ret
