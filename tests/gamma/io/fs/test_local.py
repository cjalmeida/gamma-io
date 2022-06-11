from .common import common_ops
from gamma.io.fs import get_fs_path
from pathlib import Path
import tempfile
import pytest


def test_local(fsconfig, local_path):

    with tempfile.TemporaryDirectory() as td:
        fs, base_path = get_fs_path(Path(td).as_uri())
        common_ops(fs, local_path, base_path)


def test_not_existing(fsconfig):
    with pytest.raises(Exception):
        fs, path = get_fs_path("dummyfs://bar")


def test_invalid_local(fsconfig):
    with pytest.raises(Exception):
        fs, path = get_fs_path("file://tmp")
