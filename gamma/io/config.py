"""Module abstracting configuration sources."""

#: Configuration key for datasets
DATASETS_CONFIG_KEY = "datasets"

#: Configuration key for filesystems
FILESYSTEMS_CONFIG_KEY = "filesystems"


def get_datasets_config() -> dict:
    """Return the datasets configuration as a Python dict.

    The default implementation will use the `gamma-config` library, and a `datasets`
    mapping. You can monkey-patch this function to provide your own config source.
    """
    from gamma.config import get_config, to_dict

    return to_dict(get_config()[DATASETS_CONFIG_KEY])


def get_filesystems_config() -> dict:
    """Return the filesystems configuration as a Python dict.

    The default implementation will use the `gamma-config` library, and a `datasets`
    mapping. You can monkey-patch this function to provide your own config source.
    """
    from gamma.config import get_config, to_dict

    return to_dict(get_config().get(FILESYSTEMS_CONFIG_KEY) or {})
