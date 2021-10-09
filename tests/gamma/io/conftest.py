import pytest
from gamma.config import RootConfig
from gamma.config.globalconfig import reset_config, set_config
from pathlib import Path
import tempfile


@pytest.fixture
def fsconfig(request):
    data = Path(request.module.__file__).parent / "config.yaml"
    config = RootConfig("local", data)
    set_config(config)
    yield config
    reset_config()


@pytest.fixture
def local_path():
    with tempfile.TemporaryDirectory() as lp:
        yield lp
