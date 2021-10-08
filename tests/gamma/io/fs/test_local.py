from gamma.config import get_config
from .fixtures import fsconfig, local_path
from .common import common_ops
from gamma.io.fs import get_fs_path, ops as fsop
from pathlib import Path
import tempfile


def test_local(fsconfig, local_path):

    with tempfile.TemporaryDirectory() as td:
        fs, base_path = get_fs_path(Path(td).as_uri())
        common_ops(fs, local_path, base_path)
