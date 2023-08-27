"""I/O helper module.

When adding support for new I/O operations, please follow the convention and expose
the relevant functions here.
"""

# isort: skip_file
# ruff: noqa: F401 E402

# initialize a scoped dispatcher

from plum import Dispatcher

dispatch = Dispatcher()

from .__version__ import __version__

# Core features
from ._dataset import get_dataset, get_dataset_location, read_dataset, write_dataset
from ._fs import get_fs_path, copy_dataset
from ._types import Dataset, PartitionException, DatasetException
from ._staging import is_staging_enabled

# Core dataframe libs - pandas / pyarrow
from ._pandas import read_pandas, write_pandas, list_partitions

from ._utils import try_import

# Stack specific features, you should be able to remove modules
# if you don't need specific functionality

if try_import("polars"):
    from ._polars import read_polars, write_polars

if try_import("sqlalchemy"):
    from ._sql import get_sql_engine
