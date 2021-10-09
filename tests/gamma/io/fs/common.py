from fsspec.spec import AbstractFileSystem
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
    fs.makedirs(parent, exist_ok=True)

    # test stream copy
    with open(dummy, "rb") as src:
        with fs.open(remote, "wb") as dst:
            shutil.copyfileobj(src, dst)

    got = fs.cat_file(remote)
    assert got == dummy.read_bytes()

    # test upload to file
    dummy_path = str(dummy.absolute())
    remote2 = parent + "/dummy2"
    fs.put_file(dummy_path, remote2)
    got = fs.cat_file(remote2)
    assert got == dummy.read_bytes()

    # test get file to local
    dummy2 = dummy.parent / "dummy2"
    fs.get_file(remote2, str(dummy2))
    got = dummy2.read_bytes()
    assert got == dummy.read_bytes()
