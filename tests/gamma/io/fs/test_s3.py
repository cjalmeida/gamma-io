import pytest
from gamma.io.fs import get_fs_path
from .common import common_ops
import uuid


@pytest.mark.integration
@pytest.mark.minio
def test_minio_s3(fsconfig, local_path, monkeypatch):
    """Tests for S3 FS using MinIO

    This requires a running MinIO instance, you can get one running
    with:
        docker run --rm -it -p 9000:9000 minio/minio server /data

    Disable by default, run with `pytest -m minio`
    """

    bucket = uuid.uuid4().hex[:10]
    monkeypatch.setenv("BUCKET", bucket)

    base_uri = f"s3://localhost:9000/{bucket}"
    fs, base_path = get_fs_path(base_uri)
    common_ops(fs, local_path, base_path)
