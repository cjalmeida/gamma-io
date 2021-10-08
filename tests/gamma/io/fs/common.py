import tempfile
from fsspec.spec import AbstractFileSystem
import pytest
from pathlib import Path
from gamma.io.fs import ops
import shutil


def _create_dummy_file(local_path) -> Path:
    data = b"hello world"
    out = Path(local_path) / "dummy"
    out.write_bytes(data)
    return out


def common_ops(fs: AbstractFileSystem, local_path: str, base_path: str):

    base_path = base_path.rstrip("/")

    dummy = _create_dummy_file(local_path).absolute()
    remote = base_path + "/tmp/dummy1"

    parent = ops.parent_path(remote)
    ops.makedirs(fs, parent, exist_ok=True)

    # test stream copy
    with open(dummy, "rb") as src:
        with ops.open(fs, remote, "wb") as dst:
            shutil.copyfileobj(src, dst)

    got = ops.cat_file(fs, remote)
    assert got == dummy.read_bytes()

    # test upload to file
    dummy_path = str(dummy.absolute())
    remote2 = parent + "/dummy2"
    ops.put_file(fs, dummy_path, remote2)
    got = ops.cat_file(fs, remote2)
    assert got == dummy.read_bytes()

    # test get file to local
    dummy2 = dummy.parent / "dummy2"
    ops.get_file(fs, remote2, str(dummy2))
    got = dummy2.read_bytes()
    assert got == dummy.read_bytes()
