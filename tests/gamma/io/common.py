import re
from itertools import cycle
from random import choice

import pandas as pd
import polars as pl
from plum import Dispatcher

from gamma.io import Dataset, get_fs_path, list_partitions

dispatch = Dispatcher()


def _get_l1_l2(df):
    vals_l1 = cycle("ABCD")
    l1 = [next(vals_l1) for _ in range(len(df))]
    l2 = [choice("AB") for _ in range(len(df))]
    return l1, l2


@dispatch
def assign_partitions(df: pd.DataFrame) -> pd.DataFrame:
    # assign partition values
    l1, l2 = _get_l1_l2(df)
    df = df.copy()
    df["l1"] = l1
    df["l2"] = l2
    return df


@dispatch
def assign_partitions(df: pl.DataFrame) -> pl.DataFrame:
    l1, l2 = _get_l1_l2(df)
    return df.with_columns(pl.Series("l1", l1), pl.Series("l2", l2))


def check_partitions(ds: Dataset):
    # inspect partitions
    fs, path = get_fs_path(ds)

    assert fs.isdir(path)
    folders = fs.glob(path + "/*/*")
    assert len(folders) > 1
    for f in folders:
        assert fs.isdir(f)
        assert re.match(".*/l1=[ABCD]/l2=[AB]$", f)

    # try list only partitions
    p1 = list_partitions(ds)
    assert set(p1.columns) == {"l1", "l2"}
    assert "A" in p1["l1"].tolist()
    assert "B" in p1["l1"].tolist()
    assert "C" in p1["l1"].tolist()
    assert "A" in p1["l2"].tolist()
    assert "C" not in p1["l2"].tolist()

    # list partition using filters
    p2 = list_partitions(ds, l1="A")
    assert "A" in p2["l1"].tolist()
    assert "B" not in p2["l1"].tolist()
    assert "A" in p2["l2"].tolist() or "B" in p2["l2"].tolist()


@dispatch
def check_df_equal(df1: pd.DataFrame, df2: pd.DataFrame):
    # ensure same order, drop useless index, fix categoricals, check equal
    df1 = df1.sort_values("Index").reset_index(drop=True)
    df2 = df2.sort_values("Index").reset_index(drop=True)
    df2.l1 = df2.l1.astype("str")
    df2.l2 = df2.l2.astype("str")
    pd.testing.assert_frame_equal(df1, df2, check_categorical=False)


@dispatch
def check_df_equal(df1: pl.DataFrame, df2: pl.DataFrame):
    return check_df_equal(df1.to_pandas(), df2.to_pandas())
