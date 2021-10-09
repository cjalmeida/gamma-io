from gamma.config.globalconfig import get_config

from gamma.dispatch import Val, dispatch

from gamma.config import to_dict
from .dataset import Dataset

# Default source of filesystems info
GammaConfigSource = Val["gamma-config"]
DEFAULT_SOURCE = GammaConfigSource

# Config key to use when loading filesystems information
CONFIG_KEY = "datasets"


@dispatch
def get_dataset_entry(group: str, name: str) -> Dataset:
    return get_dataset_entry(DEFAULT_SOURCE, group, name)


@dispatch
def get_dataset_entry(_: GammaConfigSource, group: str, name: str) -> Dataset:
    datasets = get_config()[CONFIG_KEY]
    if group not in datasets:
        raise KeyError(
            f"Missing '{group}' key in '{CONFIG_KEY}' configuration for datasets"
        )

    if name not in datasets[group]:
        raise KeyError(
            f"Missing '{name}' key in '[{CONFIG_KEY}][{group}]' configuration for "
            "datasets"
        )

    entry = to_dict(datasets[group][name])
    return Dataset(**entry)
