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

    #: Dataset layer name
    layer: str

    #: Dataset name, unique in a layer
    name: str

    #: URL representing the location of this library
    location: str

    #: Path params interpolated in the location
    path_params: Optional[dict] = {}

    #: The dataset format
    format: Optional[str] = "parquet"

    #: Args passed directly to the loader
    args: Optional[dict] = {}

    #: Limit the columns to load for loaders that support this feature
    columns: Optional[list[str]] = None

    #: Partition declaration if supported
    partition_by: Optional[list[str]] = []

    #: Partition values
    partitions: Optional[dict] = {}

    def __getitem__(self, item):
        return getattr(self, item)

    def __contains__(self, key):
        return hasattr(self, key)
