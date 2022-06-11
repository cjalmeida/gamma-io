from typing import Type
from gamma.dispatch import dispatch


class OutputType:
    """Base class for data output types (eg. bytes, pd.DataFrame, etc.)."""

    pass


@dispatch
def get_output_type(clazz) -> Type:
    raise NotImplementedError(f"Cannot find output type for class: {clazz}")


class BytesOutputType:
    """Raw bytes output type."""

    aliases = [bytes]


@dispatch
def get_output_type(clazz: bytes) -> Type:
    return BytesOutputType
