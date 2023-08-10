import logging
import re
import tempfile
from itertools import cycle

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

    # assign sequential cluster values
    vals = cycle("ABCD")
    cluster = [next(vals) for _ in range(len(df))]
    df = df.with_columns(pl.Series("cluster", cluster))

    # write partitioned parquet
    write_polars(df, "raw", "customers")

    assert "writing" in caplog.text.lower()
    assert "raw.customers" in caplog.text.lower()
    caplog.clear()

    # inspect partitions
    ds = get_dataset("raw", "customers")
    fs, path = get_fs_path(ds)
    for entry in fs.ls(path):
        assert re.match(".*/cluster=[ABCD]$", entry)

    # read it back
    df2 = read_polars("raw", "customers")

    assert "reading" in caplog.text.lower()
    assert "raw.customers" in caplog.text.lower()
    caplog.clear()

    # ensure same order
    df = df.sort("Index")
    df2 = df2.sort("Index")

    pd.testing.assert_frame_equal(df.to_pandas(), df2.to_pandas())

    # save and read back as feather
    write_polars(df, "raw", "customers_feather")

    # inspect partitions
    ds = get_dataset("raw", "customers_feather")
    fs, path = get_fs_path(ds)
    assert len(fs.ls(path)) == 4
    for entry in fs.ls(path):
        assert re.match(".*/cluster=[ABCD]$", entry)

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
