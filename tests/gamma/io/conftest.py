import logging
import os
import tempfile
from pathlib import Path

import pytest
from gamma.config import get_config
from gamma.config.globalconfig import reset_config


@pytest.fixture
def io_config(monkeypatch):
    curdir = Path(__file__).parent
    sample_dir = str((curdir / "sample").absolute())
    monkeypatch.setenv("GAMMA_CONFIG_ROOT", sample_dir)

    with tempfile.TemporaryDirectory() as td:
        monkeypatch.setenv("IO_TEST_TMP", td)

        yield {"temp_dir": td}

    reset_config()
