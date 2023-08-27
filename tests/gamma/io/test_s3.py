import logging
import os
import signal
import subprocess
from itertools import cycle
from random import choice

import boto3
import pytest

from gamma.io import copy_dataset, get_dataset, get_fs_path, read_pandas, write_pandas

logger = logging.getLogger("gamma.io")


@pytest.fixture(scope="session")
def localstack():
    """Fixture to initialize localstack S3 service.

    Localstack is started in docker mode.
    """

    logger.info("Check if localstack is running")
    cp = subprocess.run(["localstack", "wait", "-t", "5"], check=False)

    proc = None
    if cp.returncode != 0:
        logger.info("Start the localstack process")
        proc = subprocess.Popen(["localstack", "start"], encoding="utf-8")

        # wait until it's ready, 15s timeout
        logger.info("Waiting localstack ready")
        subprocess.run(["localstack", "wait", "-t", "15"], check=True)

    logger.info(f"Localstack ready!")

    endpoint = "http://localhost:4566"
    region = "us-east-1"

    # dummy keys
    os.environ["AWS_ACCESS_KEY_ID"] = "AKIAIOSFODNN7EXAMPLE"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

    yield dict(endpoint=endpoint, region=region)

    del os.environ["AWS_ACCESS_KEY_ID"]
    del os.environ["AWS_SECRET_ACCESS_KEY"]

    # shutdown localstack
    logger.info(f"Shutting down localstack.")
    if proc:
        proc.send_signal(signal.SIGINT)
        ret = proc.wait()
        assert ret == 0, f"Localstack shutdown returned non-zero code: {ret}"


@pytest.fixture
def bucket_test(localstack):
    bucket = "test-bucket"
    args = dict(region_name=localstack["region"], endpoint_url=localstack["endpoint"])

    s3_client = boto3.client("s3", **args)
    s3_client.create_bucket(Bucket=bucket)

    yield bucket

    # remove bucket and all objects
    s3 = boto3.resource("s3", **args)
    bucket = s3.Bucket(bucket)
    bucket.objects.all().delete()
    bucket.delete()


def test_s3_dataset(io_config):
    ds = get_dataset("raw", "customers_s3")
    fs, path = get_fs_path(ds)
    assert path == "test-bucket/customers/"
    assert "s3" in fs.protocol


def test_s3_read_write_copy(io_config, bucket_test):
    ds = get_dataset("raw", "customers_s3")
    fs, path = get_fs_path(ds)

    # check access and empty dir
    assert len(fs.ls(path)) == 0

    df = read_pandas("source", "customers_1k_local")
    assert len(df) > 100

    # assign partition values
    vals_l1 = cycle("ABCD")
    l1 = [next(vals_l1) for _ in range(len(df))]
    l2 = [choice("AB") for _ in range(len(df))]
    df["l1"] = l1
    df["l2"] = l2

    # write partitioned parquet
    write_pandas(df, "raw", "customers_s3")

    # copy dataset
    ds2 = get_dataset("raw", "customers_s3_copy")
    _, path2 = get_fs_path(ds2)
    copy_dataset(
        ds,
        ds2,
    )

    assert len(fs.find(path)) == len(fs.find(path2))
