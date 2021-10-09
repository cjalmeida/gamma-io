from enum import Enum
from typing import Optional
from pydantic import BaseModel
from gamma.config.pydantic import ConfigStruct


class DataFormat(ConfigStruct):
    class Config:
        extra = "forbid"


class Location(ConfigStruct):
    @classmethod
    def parse_scalar(cls, value: str):
        """If provided a plain scalar, assume it's a URI."""
        return URI(uri=value)


class Compression(str, Enum):
    """Compression codecs.

    Note that all other than gzip may require extra packages.
    """

    GZIP = "gzip"
    ZSTD = "zstd"
    LZ4 = "lz4"
    SNAPPY = "snappy"
    BROTLI = "brotli"


class URI(Location):
    kind: str = "uri"
    uri: str


class Parquet(DataFormat):
    kind: str = "parquet"
    compression: Optional[Compression] = "snappy"

    # make the files Spark compatible by default
    flavor: Optional[str] = "spark"


class CSV(DataFormat):
    kind: str = "csv"
    sep: str = ","
    compression: Optional[Compression] = None


class Pickle(DataFormat):
    kind: str = "pickle"
    compression: Optional[Compression] = None


class Bytes(DataFormat):
    """Raw bytes."""

    kind: str = "bytes"
    compression: Optional[Compression] = None


class Dataset(BaseModel):
    location: Location
    format: DataFormat

    # string pointing to a
    # schema: Optional[str]
