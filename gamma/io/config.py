"""Module abstracting configuration sources."""

#: Configuration key for datasets
DATASETS_CONFIG_KEY = "datasets"

#: Configuration key for filesystems
FILESYSTEMS_CONFIG_KEY = "filesystems"


def get_dataset_config(layer: str, name: str) -> dict:
    """Return the datasets configuration as a Python dict.

    The default implementation will use the `gamma-config` library, and a `datasets`
    mapping. You can monkey-patch this function to provide your own config source.

    It should raise `KeyError` if an entry cannot be found.
    """
    _check_gamma_config()
    from gamma.config import get_config, to_dict

    return to_dict(get_config()[DATASETS_CONFIG_KEY][layer][name])


def get_filesystems_config() -> dict:
    """Return the filesystems configuration as a Python dict.

    The default implementation will use the `gamma-config` library, and a `datasets`
    mapping. You can monkey-patch this function to provide your own config source.
    """
    _check_gamma_config()
    from gamma.config import get_config, to_dict

    config = get_config().get(FILESYSTEMS_CONFIG_KEY)
    return to_dict(config) if config else {}


def _check_gamma_config():
    from ._types import MissingDependencyException

    try:
        import gamma.config  # noqa
    except ModuleNotFoundError:  # pragma: no cover
        msg = (
            "Missing 'gamma-config' dependency. Either provided it via 'pip install "
            "gamma-config', or monkey-patch 'gamma.io.config.get_dataset_config' and "
            "'gamma.io.config.get_filesystems_config'"
        )
        raise MissingDependencyException(msg)
