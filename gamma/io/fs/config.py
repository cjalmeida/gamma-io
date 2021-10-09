from typing import Dict, Optional
from copy import deepcopy
from gamma.dispatch import Val, dispatch
from pydantic import BaseModel
from gamma.io.config import ConfigType
import tempfile

# Default source of filesystems info
GammaConfigSource = Val["gamma-config"]
DEFAULT_SOURCE = GammaConfigSource

# Config key to use when loading filesystems information
CONFIG_KEY = "filesystems"

# Supported schemes
FileScheme = Val["file"]
S3Scheme = Val["s3"]

####
# config structure


class FSCacheMeta(BaseModel):
    tmp_dir: str = tempfile.gettempdir()


class FSMeta(BaseModel):
    cache: FSCacheMeta = FSCacheMeta()


class FSConfig(ConfigType):
    __discriminator__ = "scheme"
    scheme: str = "__unset__"


class FileSystems(BaseModel):
    meta: FSMeta
    filesystems: Dict[str, FSConfig]

    def __iter__(self):
        return iter(self.filesystems.items())


class LocalFS(FSConfig):
    scheme: str = "file"


class S3FS(FSConfig):
    scheme: str = "s3"
    bucket: str
    endpoint_url: Optional[str] = None
    key: Optional[str] = None
    secret: Optional[str] = None

####
# methods


@dispatch
def get_filesystems(fsdict: dict):
    """Parse a dict-of-dicts as a dict of `Filesystem` objects"""
    _dict = deepcopy(fsdict)
    meta_obj = _dict.pop("_meta", {})
    meta = FSMeta(**meta_obj)
    return FileSystems(meta=meta, filesystems=_dict)


@dispatch
def get_filesystems(_: GammaConfigSource):
    """Load filesystems config from gamma-config"""
    from gamma.config import get_config, to_dict

    config = get_config()
    fsdict = to_dict(config[CONFIG_KEY])
    return get_filesystems(fsdict)


@dispatch
def get_filesystems():
    """Get the filesystems set from the default source"""
    return get_filesystems(DEFAULT_SOURCE)
