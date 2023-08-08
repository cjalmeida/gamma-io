"""Main helper module for loading/writing data."""


import logging
import tempfile
from typing import Tuple

import fsspec

from ._fs import get_fs_path
from ._types import Dataset, PartitionException

# type alias
FSPathType = Tuple[fsspec.AbstractFileSystem, str]

logger = logging.getLogger(__name__)


def get_dataset(layer: str, name: str, *, args=None, columns=None, **params) -> Dataset:
    """Load a dataset entry from configuration.

    You can pass both partition filters or path parameters as keyword args.

    Args:
        layer: the layer name
        name: the dataset name

    Keyword Args:
        args: Optionally override loader arguments
        columns: Optionally override the columns to load, if supported
        **params: partition specs or path params to pass to the location

    """
    from .config import get_datasets_config

    entry = get_datasets_config()[layer][name]
    dataset = Dataset(layer=layer, name=name, **entry)
    dataset.args.update(args or {})

    # parse partitions in params
    for part in list(params):
        if part in dataset.partition_by:
            dataset.partitions[part] = params.pop(part)

    _validate_partitions(dataset)

    # treat the rest as path params
    dataset.path_params.update(params)

    if columns:
        dataset.columns = columns

    return dataset


def _validate_partitions(ds: Dataset) -> None:
    """Ensure we have no holes in the provided partitions."""

    matches = [part in ds.partitions for part in ds.partition_by]

    # iterating checking for invalid matches
    allow_match = True
    invalid = None
    for i, match in enumerate(matches):
        if match and allow_match:
            continue
        elif match and not allow_match:
            invalid = i
            break
        elif not match:
            allow_match = False
            continue

    if invalid is not None:
        msg = (
            f"Incorrect partition provided. We got {ds.partitions} while expecting "
            f"the sequence {ds.partition_by} for dataset '{ds.layer}.{ds.name}'"
        )
        raise PartitionException(msg, ds)


def _get_partition_path(ds: Dataset):
    if not ds.partition_by:
        return ""

    parts = []
    for part in ds.partition_by:
        part
    parts = [
        f"{part}={ds.partitions.get(part)}"
        for part in ds.partition_by
        if part in ds.partitions
    ]

    return "/".join(parts)


def get_dataset_location(ds: Dataset) -> str:
    """Get the dataset location with path params applied."""
    try:
        base_path = ds.location.format(**ds.path_params).rstrip("/")
        part_path = _get_partition_path(ds)
        return f"{base_path}/{part_path}"

    except KeyError as ex:
        raise KeyError(
            f"Missing Dataset param '{ex.args[0]}' while trying to render location "
            f"URI: {ds.location}"
        )


def copy_dataset(src: Dataset, dst: Dataset):
    """Copy dataset from `src` into `dst`.

    If not DataFrame operation is needed, this is generally more efficient. This
    function may fail if there's no known transformation between `src` and `dst`
    formats.
    """
    from fsspec.callbacks import TqdmCallback

    from ._pandas import read_pandas, write_pandas

    if src.format != dst.format:
        logger.warn(
            f"No known quick conversion between src/dst formats: "
            f"{src.format}/{dst.format}. Falling back to Pandas read/write"
        )
        write_pandas(read_pandas(src), dst)

    loc1 = get_dataset_location(src)
    loc2 = get_dataset_location(dst)

    path2: str
    fs1, path1 = get_fs_path(src)
    fs2, path2 = get_fs_path(dst)

    logger.info(f"Copying dataset from {loc1} to {loc2}.")

    with tempfile.TemporaryDirectory() as td:
        if fs1.isfile(path1):
            fs1.get(path1, f"{td}/data")
            fs2.makedirs(_parent(path2))
            fs2.put(f"{td}/data", path2)
        else:
            fs1.get(path1, f"{td}/", recursive=True, callback=TqdmCallback())
            fs2.put(f"{td}/", path2, recursive=True, callback=TqdmCallback())


def _parent(path: str):
    if "/" not in path:
        return ""

    p, *_ = path.rstrip("/").rsplit("/", 1)
    return p
