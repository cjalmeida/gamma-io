"""Module for manipulating Dataset objects."""


import logging
from typing import Tuple

import fsspec

from ._types import Dataset, PartitionException

# type alias
FSPathType = Tuple[fsspec.AbstractFileSystem, str]

logger = logging.getLogger("gamma.io")


def get_dataset(
    _layer: str, _name: str, *, args=None, columns=None, **params
) -> Dataset:
    """Load a dataset entry from configuration.

    You can pass both partition filters or path parameters as keyword args.

    Args:
        _layer: the layer name
        _name: the dataset name

    Keyword Args:
        args: Optionally override reader/writer arguments
        columns: Optionally override the columns to load, if supported
        **params: partition specs or path params to pass to the location

    """
    from .config import get_datasets_config

    entry = get_datasets_config()[_layer][_name]
    dataset = Dataset(layer=_layer, name=_name, **entry)
    dataset.args.update(args or {})

    # parse partitions in params
    for part in list(params):
        if part in dataset.partition_by:
            dataset.partitions[part] = params.pop(part)

    _validate_partitions(dataset)

    # treat the rest as location params
    dataset.params.update(params)

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


def get_dataset_location(ds: Dataset) -> str:
    """Get the dataset location with path params applied."""
    try:
        base_path = ds.location.format(**ds.params)
        return base_path

    except KeyError as ex:  # pragma: no cover
        raise KeyError(
            f"Missing Dataset param '{ex.args[0]}' while trying to render location "
            f"URI: {ds.location}"
        )
