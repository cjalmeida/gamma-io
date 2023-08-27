"""Module abstracting configuration sources."""

from ._types import StagingConf

#: Configuration key for datasets
DATASETS_CONFIG_KEY = "datasets"

#: Configuration key for filesystems
FILESYSTEMS_CONFIG_KEY = "filesystems"


def get_dataset_config(layer: str, name: str) -> dict:
    """Return the datasets configuration as a Python dict.

    The default implementation will use the `gamma-config` library, and a `datasets`
    mapping. You can monkey-patch `_resolve_dataset_config` to provide your own
    config source.

    It should raise `KeyError` if an entry cannot be found.
    """
    return _resolve_dataset_config(layer, name)


def get_filesystems_config() -> dict:
    """Return the filesystems configuration as a Python dict.

    The default implementation will use the `gamma-config` library, and a `filesystems`
    config entry. You can monkey-patch `_resolve_filesystems_config` to provide your own
    config source.
    """
    return _resolve_filesystems_config()


def get_staging_config() -> StagingConf:
    """Return the staging configuration.

    The default implementation will use the `gamma-config` library, and a
    `datasets._staging` mapping. You can monkey-patch `_resolve_filesystems_config` to
    provide your own config source.
    """
    conf = _resolve_staging_config()
    return StagingConf(**conf)


def _check_gamma_config():
    from ._types import MissingDependencyException

    try:
        import gamma.config  # noqa
    except ModuleNotFoundError:  # pragma: no cover
        msg = (
            "Missing 'gamma-config' dependency. Either provided it via 'pip install"
            " gamma-config', or monkey-patch 'gamma.io.config._resolve_*' functions"
        )
        raise MissingDependencyException(msg)


###
# Override these to provide your own configuration
###


def _resolve_dataset_config(layer: str, name: str) -> dict:
    """Resolve dataset config using `gamma-config`.

    Monkey-patch this to provide your own resolver.
    """
    _check_gamma_config()
    from gamma.config import get_config, to_dict

    return to_dict(get_config()[DATASETS_CONFIG_KEY][layer][name])


def _resolve_filesystems_config() -> dict:
    """Return the filesystems configuration as a Python dict.

    The default implementation will use the `gamma-config` library, and a `datasets`
    mapping. You can monkey-patch this function to provide your own config source.
    """
    _check_gamma_config()
    from gamma.config import get_config, to_dict

    config = get_config().get(FILESYSTEMS_CONFIG_KEY)
    return to_dict(config) if config else {}


def _resolve_staging_config() -> dict:
    """Return the dataset staging configuration using `gamma-config`.

    Monkey-patch this to provide your own resolver.
    """
    _check_gamma_config()
    from gamma.config import get_config, to_dict

    config = get_config()[DATASETS_CONFIG_KEY].get("_staging")
    return to_dict(config) if config else {}
