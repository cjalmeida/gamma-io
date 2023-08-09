"""I/O helper module.

When adding support for new I/O operations, please follow the convention and expose
the relevant functions here.
"""
# isort: skip_file
# ruff: noqa: F401

# initialize a scoped dispatcher

from plum import Dispatcher

dispatch = Dispatcher()

from ._core import get_dataset, get_dataset_location
from ._fs import get_fs_path
from ._types import Dataset, PartitionException, DatasetException

from ._polars import read_polars, write_polars
from ._pandas import read_pandas, write_pandas
