from pathlib import Path

import pandas as pd
from sqlalchemy import CursorResult, text
from sqlalchemy_utils import create_database, database_exists

from gamma.io import get_dataset, get_sql_engine, read_pandas, write_pandas


def test_sql_read_write(io_config, caplog):
    # load local dataset csv/zip
    df = read_pandas("source", "customers_1k_local")

    # create database folder
    sql_folder = Path(io_config["temp_dir"]) / "sql"
    sql_folder.mkdir(parents=True)

    # create database first
    ds = get_dataset("raw", "customers_sql_table")
    engine = get_sql_engine(ds)
    create_database(engine.url)
    assert database_exists(engine.url)

    # test datetimes
    df["Subscription Date"] = pd.to_datetime(df["Subscription Date"])

    # write to SQL database table
    write_pandas(df, "raw", "customers_sql_table")

    # check we wrote to db
    with engine.begin() as conn:
        cur: CursorResult = conn.execute(text("select count(*) as num from customers"))
        table_count = cur.scalar_one()
        assert table_count == len(df)

    # check we can read the full table from db
    df2 = read_pandas(ds)

    df = df.sort_values("Index").reset_index(drop=True)
    df2 = df2.sort_values("Index").reset_index(drop=True)

    pd.testing.assert_frame_equal(df, df2)

    # check we can read a parameterized SQL query
    df3 = read_pandas("raw", "customers_sql_query", name="andr%")

    for _name in df3["First Name"].tolist():
        assert "andr" in _name.lower()
