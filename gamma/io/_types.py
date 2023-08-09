from typing import Optional

from pydantic import BaseModel


class DatasetException(Exception):
    """Base exception for wrong dataset specifications."""

    def __init__(self, msg: str, ds: "Dataset") -> None:
        super().__init__(msg)
        self.ds = ds


class PartitionException(DatasetException):
    """Raised on partition related errors."""


class Dataset(BaseModel):
    """Structure for dataset entries."""

    layer: str
    """Dataset layer name"""

    name: str
    """Dataset name, unique in a layer"""

    protocol: str
    """The dataset storage protocol. If not provided in declarative
    configuration, it's inferred from location URL scheme."""

    location: str
    """URL representing the location of this library"""

    params: Optional[dict] = {}
    """Params to be interpolated in the location URI. Provided on dataset instantiation."""

    format: str
    """The dataset storage format."""

    read_args: Optional[dict] = {}
    """Extra arguments passed directly to the reader."""

    write_args: Optional[dict] = {}
    """Extra arguments passed directly to the writer."""

    #: Limit the columns to load for loaders that support this feature
    columns: Optional[list[str]] = None

    #: Partition declaration if supported
    partition_by: Optional[list[str]] = []

    #: Partition values
    partitions: Optional[dict] = {}

    compression: Optional[str] = None
    """Compression, if supported by the loader/format."""


class ParquetDataset(Dataset):
    pass
