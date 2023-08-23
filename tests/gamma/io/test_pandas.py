import gzip
import logging
import re
from itertools import cycle
from random import choice

import pandas as pd

from gamma.io import (
    get_dataset,
    get_fs_path,
    list_partitions,
    read_pandas,
    write_pandas,
)


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

    # assign partition values
    vals_l1 = cycle("ABCD")
    l1 = [next(vals_l1) for _ in range(len(df))]
    l2 = [choice("AB") for _ in range(len(df))]
    df["l1"] = l1
    df["l2"] = l2

    # write partitioned parquet
    write_pandas(df, "raw", "customers")

    assert "writing" in caplog.text.lower()
    assert "raw.customers" in caplog.text.lower()
    caplog.clear()

    # inspect partitions
    ds = get_dataset("raw", "customers")
    fs, path = get_fs_path(ds)
    for entry in fs.glob(path + "/*/*"):
        assert re.match(".*/l1=[ABCD]/l2=[AB]$", entry)

    # try list only partitions
    p1 = list_partitions("raw", "customers")
    assert "A" in p1["l1"].tolist()
    assert "B" in p1["l1"].tolist()
    assert "C" in p1["l1"].tolist()
    assert "A" in p1["l2"].tolist()
    assert "C" not in p1["l2"].tolist()

    # list partition using filters
    p2 = list_partitions("raw", "customers", l1="A")
    assert "A" in p2["l1"].tolist()
    assert "B" not in p2["l1"].tolist()
    assert "A" in p2["l2"].tolist() or "B" in p2["l2"].tolist()

    # read it back
    df2 = read_pandas("raw", "customers")

    # ensure same order, drop useless index, fix categoricals, check equal
    df = df.sort_values("Index").reset_index(drop=True)
    df2 = df2.sort_values("Index").reset_index(drop=True)
    df2.l1 = df2.l1.astype("str")
    df2.l2 = df2.l2.astype("str")
    pd.testing.assert_frame_equal(df, df2, check_categorical=False)

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
