import gzip
import logging

import pandas as pd
import polars as pl
import pytest

from gamma.io import copy_dataset, get_dataset, get_fs_path, read_dataset, write_dataset

from .common import assign_partitions, check_df_equal, check_partitions

DataFrame = pd.DataFrame | pl.DataFrame

df_classes = [pd.DataFrame, pl.DataFrame]


@pytest.mark.parametrize("df_cls", df_classes)
def test_remote_logs(io_config, caplog, df_cls):
    caplog.set_level(logging.INFO)

    # load remote dataset csv/zip
    df: DataFrame = read_dataset(df_cls, "source", "customers_1k_plain")
    df = assign_partitions(df)

    assert len(df) > 100
    assert "reading" in caplog.text.lower()
    assert "source.customers_1k" in caplog.text.lower()

    # write dataset
    ds = get_dataset("raw", "customers_parquet")
    write_dataset(df, ds)

    assert "writing" in caplog.text.lower()
    assert "raw.customers" in caplog.text.lower()
    caplog.clear()


@pytest.mark.parametrize("df_cls", [pd.DataFrame])
def test_csv_zip(io_config, df_cls):
    # compressed csvs only supported in pandas
    df = read_dataset(df_cls, "source", "customers_1k_local")
    df = assign_partitions(df)

    # save as gzip csv
    write_dataset(df, "raw", "customers_csv_gz")

    # inspect file
    ds = get_dataset("raw", "customers_csv_gz")
    fs, path = get_fs_path(ds)
    assert fs.isfile(path)

    # read as gzip stream
    with fs.open(path) as fo:
        with gzip.open(fo) as gzip_fo:
            df3 = pd.read_csv(gzip_fo)

    pd.testing.assert_frame_equal(df, df3)

    # save as excel
    ds = get_dataset("raw", "customers_excel")
    write_dataset(df, ds)
    fs, path = get_fs_path(ds)
    assert fs.isfile(path)
    assert path.endswith(".xlsx")


@pytest.mark.parametrize("df_cls", df_classes)
@pytest.mark.parametrize("fmt", ["parquet", "feather"])
def test_parquet_feather(io_config, df_cls, fmt):
    df = read_dataset(df_cls, "source", "customers_1k_local_plain")
    df = assign_partitions(df)

    # save/read as partitioned dataset
    ds = get_dataset("raw", f"customers_{fmt}")

    write_dataset(df, ds)
    check_partitions(ds)

    df2 = read_dataset(df_cls, ds)
    check_df_equal(df, df2)

    # save/read as forced single-file dataset
    ds = get_dataset("raw", f"customers_{fmt}_single")
    write_dataset(df, ds)
    fs, path = get_fs_path(ds)
    assert fs.isfile(path)
    assert path.endswith("_single")
    df2 = read_dataset(df_cls, ds)
    check_df_equal(df, df2)

    # cleanup for next test
    fs.rm_file(path)

    # save/read as heuristic single-file feather, treating location as folder
    # also test the overriding get_dataset attribute
    ds = get_dataset("raw", f"customers_{fmt}_single", single_file=None)
    write_dataset(df, ds)
    fs, path = get_fs_path(ds)

    # path should still point to a file as we're not doing partitioning
    assert fs.isfile(path)

    # assert we have a nice name for the partition
    assert path.endswith(f"/data.{fmt}")

    df2 = read_dataset(df_cls, ds)
    check_df_equal(df, df2)


@pytest.mark.parametrize("df_cls", df_classes)
def test_dynamic(io_config, df_cls):
    df = read_dataset(df_cls, "source", "customers_1k_local_plain")

    # save dynamic
    write_dataset(df, "run", "customers_dyn")

    # inspect file
    ds = get_dataset("run", "customers_dyn")
    fs, path = get_fs_path(ds)
    assert fs.isfile(path)
    assert path.endswith(".parquet")

    # save dynamic overriding format, params
    ds = get_dataset("run", "customers_dyn", format="csv", ext="csv")
    write_dataset(df, ds)
    fs, path = get_fs_path(ds)
    assert fs.isfile(path)
    assert path.endswith(".csv")


def test_copy_across(io_config):
    ds1 = get_dataset("source", "customers_1k_plain")
    ds2 = get_dataset("raw", "customers_csv_plain")
    copy_dataset(ds1, ds2)

    fs, path = get_fs_path(ds2)
    assert fs.isfile(path)
