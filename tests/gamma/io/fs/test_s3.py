import atexit
import os
import uuid
import socket

import pytest

from gamma.io.fs import get_fs_path
from tests.netutils import random_port
from gamma.config import config_context

from .common import common_ops


@pytest.fixture(scope="session")
def minio_server():
    """
    Starts a new MinIO instance via Docker and configure the filesystem config.

    This requires a working Docker instance.
    """

    port = random_port()
    name = f"gamma-io-test-{port}"

    # start minio
    cmd = f"docker run -d --rm -p {port}:9000 --name {name} minio/minio server /data"
    if os.system(cmd) != 0:
        raise Exception("Could not start Minio Docker instance. Check logs.")
    atexit.register(lambda: os.system(f"docker stop {name}"))
    yield {"port": port}


@pytest.fixture
def minio(minio_server, fsconfig):

    port = minio_server["port"]
    # random bucket
    bucket = uuid.uuid4().hex[:10]

    # update config
    endpoint_url = f"http://localhost:{port}"
    base_uri = f"s3://localhost:{port}/{bucket}"

    cfg = dict(
        filesystems=dict(
            s3test=dict(
                endpoint_url=endpoint_url,
                bucket=bucket,
            )
        )
    )
    with config_context(cfg):
        yield base_uri


@pytest.mark.integration
@pytest.mark.minio
def test_minio_s3(local_path, minio, monkeypatch):
    """Tests for S3 FS using MinIO

    This will start a new MinIO instance via Docker, see `minio`.

    Disable by default, run with `pytest -m minio`
    """

    fs, base_path = get_fs_path(minio)
    common_ops(fs, local_path, base_path)
