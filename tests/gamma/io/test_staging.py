import pandas as pd
import polars as pl
import pytest

from gamma.io import (
    get_dataset,
    get_fs_path,
    is_staging_enabled,
    read_dataset,
    write_dataset,
)
from gamma.io._staging import (
    get_stage_reader_fs_path,
    get_stage_writer_fs_path,
    get_staging_config,
    use_staging,
)

from .common import assign_partitions, check_df_equal

df_classes = [pd.DataFrame, pl.DataFrame]
DataFrame = pd.DataFrame | pl.DataFrame


@pytest.mark.parametrize("df_cls", df_classes)
@pytest.mark.parametrize("fmt", ["parquet", "feather", "csv"])
def test_stage_read_write(io_config, df_cls, fmt, monkeypatch):
    ds = get_dataset("raw", f"customers_{fmt}")

    # default disabled
    assert not is_staging_enabled()

    # writer fs_path should return regular path
    _, path = get_stage_writer_fs_path(ds)
    assert "/stage/" not in path

    # reader return regular path
    _, path = get_stage_reader_fs_path(ds)
    assert "/stage/" not in path

    monkeypatch.setenv("IO_TEST_STAGE", "true")

    # enabled via config
    assert is_staging_enabled()

    # regular get_fs_path should not return staged
    _, path = get_fs_path(ds)
    assert "/stage/" not in path

    # we can force returning staged
    _, path = get_fs_path(ds, True)
    assert "/stage/" in path

    # writer now should return staged
    _, path = get_stage_writer_fs_path(ds)
    assert "/stage/" in path

    # reader return regular because dataset not there yet
    _, path = get_stage_reader_fs_path(ds)
    assert "/stage/" not in path

    # read the test dataset
    df = read_dataset(df_cls, "source", "customers_1k_plain")
    df = assign_partitions(df)

    # write to stage
    write_dataset(df, ds)

    # reader return stage path
    _, path = get_stage_reader_fs_path(ds)
    assert "/stage/" in path

    # check if we can read
    df2 = read_dataset(df_cls, ds)
    check_df_equal(df, df2)

    # remove the data and check reader should NOT return stage path
    fs, path = get_stage_reader_fs_path(ds)
    fs.rm(path, recursive=True)
    fs, path = get_stage_reader_fs_path(ds)
    assert "/stage/" not in path


def test_context(io_config):
    ds = get_dataset("raw", f"customers_parquet")

    # default disabled
    assert not is_staging_enabled()

    # writer should not return staged
    _, path = get_stage_writer_fs_path(ds)
    assert "/stage/" not in path

    with use_staging():
        assert is_staging_enabled()

        _, path = get_stage_writer_fs_path(ds)
        assert "/stage/" in path


def test_no_config():
    from gamma.config import RootConfig, get_config, to_dict
    from gamma.config.globalconfig import set_config

    cfg_str = """
    datasets: {}
    """

    cfg = RootConfig("dummy", cfg_str)
    set_config(cfg)
    assert get_staging_config()
