import logging
import re
import tempfile
from itertools import cycle
from random import choice

import pandas as pd
import polars as pl

from gamma.io import get_dataset, get_fs_path, read_pandas, read_polars, write_polars


def test_read_write(io_config, caplog):
    caplog.set_level(logging.INFO)

    df: pl.DataFrame
    df2: pl.DataFrame

    # polars has no direct support for compressed csvs
    df = read_polars("source", "customers_1k_plain")
    assert len(df) > 100

    # assign partition values
    vals_l1 = cycle("ABCD")
    l1 = [next(vals_l1) for _ in range(len(df))]
    l2 = [choice("AB") for _ in range(len(df))]
    df = df.with_columns(pl.Series("l1", l1), pl.Series("l2", l2))

    # write partitioned parquet
    write_polars(df, "raw", "customers")

    assert "writing" in caplog.text.lower()
    assert "raw.customers" in caplog.text.lower()
    caplog.clear()

    # inspect partitions
    ds = get_dataset("raw", "customers")
    fs, path = get_fs_path(ds)
    for entry in fs.glob(path + "/*/*"):
        assert re.match(".*/l1=[ABCD]/l2=[AB]$", entry)

    # read it back
    df2 = read_polars("raw", "customers")

    assert "reading" in caplog.text.lower()
    assert "raw.customers" in caplog.text.lower()
    caplog.clear()

    # ensure same order
    df = df.sort("Index")
    df2 = df2.sort("Index")
    df2 = df2.with_columns(pl.col("l1").cast(pl.Utf8), pl.col("l2").cast(pl.Utf8))

    pd.testing.assert_frame_equal(df.to_pandas(), df2.to_pandas())

    # save and read back as feather
    write_polars(df, "raw", "customers_feather")

    # inspect partitions
    ds = get_dataset("raw", "customers_feather")
    fs, path = get_fs_path(ds)
    for entry in fs.glob(path + "/*/*"):
        assert re.match(".*/l1=[ABCD]/l2=[AB]$", entry)

    df3 = read_polars("raw", "customers_feather")
    df3 = df3.sort("Index")
    pd.testing.assert_frame_equal(df.to_pandas(), df3.to_pandas())

    # save as csv file
    ds = get_dataset("raw", "customers_csv_plain")
    fs, path = get_fs_path(ds)
    write_polars(df, ds)
    df4 = read_polars(ds)
    assert fs.isfile(path)
    pd.testing.assert_frame_equal(df.to_pandas(), df4.to_pandas())
