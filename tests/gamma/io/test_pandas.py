import gzip
import logging
import re
from itertools import cycle

import pandas as pd

from gamma.io import get_dataset, get_fs_path, read_pandas, write_pandas


def test_read_write(io_config, caplog):
    caplog.set_level(logging.INFO)

    df: pd.DataFrame
    df2: pd.DataFrame

    # load remote dataset csv/zip
    df = read_pandas("source", "customers_1k")

    assert len(df) > 100

    assert "reading" in caplog.text.lower()
    assert "source.customers_1k" in caplog.text.lower()
    caplog.clear()

    # assign sequential cluster values
    vals = cycle("ABCD")
    cluster = [next(vals) for _ in range(len(df))]
    df["cluster"] = cluster

    # write partitioned parquet
    write_pandas(df, "raw", "customers")

    assert "writing" in caplog.text.lower()
    assert "raw.customers" in caplog.text.lower()
    caplog.clear()

    # inspect partitions
    ds = get_dataset("raw", "customers")
    fs, path = get_fs_path(ds)
    for entry in fs.ls(path):
        assert re.match(".*/cluster=[ABCD]$", entry)

    # read it back
    df2 = read_pandas("raw", "customers")

    # ensure same order, drop useless index, check equal
    df = df.sort_values("Index").reset_index(drop=True)
    df2 = df2.sort_values("Index").reset_index(drop=True)
    pd.testing.assert_frame_equal(df, df2)

    # save as csv
    write_pandas(df, "raw", "customers_csv")

    # inspect file
    ds = get_dataset("raw", "customers_csv")
    fs, path = get_fs_path(ds)

    assert fs.isfile(path)

    # read as gzip stream
    with fs.open(path) as fo:
        with gzip.open(fo) as gzip_fo:
            df3 = pd.read_csv(gzip_fo)

    pd.testing.assert_frame_equal(df, df3)

    # save as json
    write_pandas(df, "raw", "customers_excel")
