"""Module for manipulating Dataset objects."""


import logging
from typing import Tuple

import fsspec

from . import dispatch
from ._types import Dataset

# type alias
FSPathType = Tuple[fsspec.AbstractFileSystem, str]

logger = logging.getLogger("gamma.io")


def get_dataset(
    layer: str,
    name: str,
    **kwargs,
) -> Dataset:
    """Load a dataset entry from configuration.

    If the layer has a `_dynamic` dataset, you can create "dynamic" datasets using the
    values inside the `_dynamic` configuration block as defaults, and override the
    configuration in the keyword arguments as needed.

    Keyword arguments can override in the dataset configuration specification (eg.
    `location`, `format`, `args`, etc.). Note `format` can only be overridden
    if there's a `_dynamic` entry in layer config; `layer` and  `location` cannot be
    overridden. See [gamma.io.Dataset][] for the fields.

    For `params`, `args`, `*_args`, fields, provided keyword arguments will be merged
    with existing config.

    All keyword arguments can be used to dynamically render the `location` field.

    Any keyword argument that match a partition column (see
    [gamma.io.Dataset#partition_by][]) will be treated as a partition key.

    Args:
        layer: The layer name
        name: The dataset name
        **kwargs: See above
    """
    cfg, _ = _resolve_ds_config(layer, name, kwargs)

    fields = set(Dataset.model_fields)

    # handle dataset field overrides
    for key in list(kwargs):
        if key in fields:
            if key in ("args", "params") or key.endswith("_args"):
                # merge args fields
                cfg.setdefault(key, {}).update(kwargs[key] or {})
            else:
                # simple replace
                cfg[key] = kwargs[key]

    # instantiate the dataset
    dataset = Dataset(layer=layer, name=name, **cfg)

    # handle partitions in params
    for part in list(kwargs):
        if part in dataset.partition_by:
            dataset.partitions[part] = kwargs[part]

    # kwargs as partition params
    dataset.params.update(kwargs)

    return dataset


@dispatch
def read_dataset(cls, *args, **kwargs):
    raise NotImplementedError(f"Cannot read dataset returning type {cls}")


@dispatch
def write_dataset(data, *args, **kwargs):
    raise NotImplementedError(f"Cannot write dataset with input type {type(data)}")


def _resolve_ds_config(layer: str, name: str, kwargs: dict) -> dict:
    from .config import get_dataset_config

    dynamic = False

    # try configured dataset
    try:
        cfg = get_dataset_config(layer, name)
    except KeyError:
        cfg = None

    # try dynamic dataset
    try:
        if cfg is None:
            cfg = get_dataset_config(layer, "_dynamic")
            dynamic = True
    except KeyError:
        cfg = None

    if cfg is None:
        raise ValueError(
            f"Dataset named '{layer}/{name} not found in configuration and layer "
            f"'{layer}' does not allow dynamic datasets by providing a '_dynamic' "
            "entry."
        )

    # do some checks

    if "location" in kwargs:
        raise ValueError(
            "Dataset 'location' cannot be overridden, use {params} to customize it."
        )

    if not dynamic and "format" in kwargs:
        raise ValueError(
            "Dataset 'format' can only be overridden for dynamic datasets."
        )

    return cfg, dynamic


def get_dataset_location(ds: Dataset) -> str:
    """Get the dataset location with path params applied.

    You can use any dataset field or entry in dataset `params` dict as location params.
    The raw location string is interpolated using `str.format`.
    """
    params = dict()
    params.update(ds.model_dump())
    params.update(ds.params)

    try:
        path = ds.location.format(**params)
        return path
    except KeyError as ex:  # pragma: no cover
        raise KeyError(
            f"Missing Dataset param '{ex.args[0]}' while trying to render location "
            f"URI: {ds.location}"
        )
