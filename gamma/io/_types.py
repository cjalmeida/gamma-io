from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, model_validator

ArrowFmt = Literal["feather"] | Literal["arrow"] | Literal["ipc"]

FEATHER_STRINGS = set(["feather", "arrow", "ipc"])


class DatasetException(Exception):
    """Base exception for wrong dataset specifications."""

    def __init__(self, msg: str, ds: "Dataset") -> None:
        super().__init__(msg)
        self.ds = ds


class PartitionException(DatasetException):
    """Raised on partition related errors."""


class MissingDependencyException(Exception):
    pass


class StagingConf(BaseModel):
    use_staging: bool = False
    location: str | None = None


class Dataset(BaseModel):
    """Structure for dataset entries."""

    layer: str
    """Dataset layer name"""

    name: str
    """Dataset name, unique in a layer"""

    location: str
    """URL representing the location of this dataset. The location may have parameters
    enclosed in single curly brackets (eg. `/path/to/{param}`). These params may
    reference fields in this dataset struct (eg. `layer`, `name`) or arguments to be
    provided during `Dataset` instantiation via [get_dataset][]"""

    format: str
    """The dataset storage format."""

    protocol: str | None = None
    """The dataset storage protocol. If not provided in declarative
    configuration, it's inferred from location URL scheme."""

    is_file: bool | None = None
    """This forces the location to be treated as a file (`True`), as a folder (`False`)
    or use heuristics to figure out. The heuristics rules are, in order:
        - If `partition_by` is set, treat as folder.
        - If path ends with a `/` (slash), treat as folder.
        - If path last component (eg. filename) contains a `.` (dot) treat as file
        - Otherwise, treat as folder.
    """

    params: Optional[dict] = {}
    """Params to be interpolated in the location URI, or passed as SQL query parameters.
    Provided on dataset instantiation."""

    args: Optional[dict] = {}
    """Extra arguments shared by both reader/writer."""

    read_args: Optional[dict] = {}
    """Extra arguments passed directly to the reader."""

    write_args: Optional[dict] = {}
    """Extra arguments passed directly to the writer."""

    engine_args: Optional[dict] = {}
    """Extra arguments passed to SQLAlchemy `create_engine`."""

    #: Partition declaration if supported
    partition_by: Optional[list[str]] = []

    #: Partition values
    partitions: Optional[dict] = {}

    model_config = ConfigDict(extra="forbid")

    @model_validator(mode="after")
    def _parse_protocol(self) -> "Dataset":
        """Set protocol field from location if not provided."""
        from urllib.parse import urlsplit

        if self.protocol is not None:
            return self

        u = urlsplit(self.location)
        self.protocol = u.scheme
        return self


class ParquetDataset(Dataset):
    pass
