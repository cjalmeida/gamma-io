from gamma.io.catalog.config import Dataset
from gamma.io.catalog import get_dataset_entry


def test_ds1(fsconfig):
    ds: Dataset = get_dataset_entry("group1", "ds1")
    assert ds.location.kind == 'uri'
    assert ds.format.kind == 'csv'
    assert ds.format.sep == ','


def test_ds2(fsconfig):
    ds: Dataset = get_dataset_entry("group1", "ds2")
    assert ds.location.kind == 'uri'
    assert ds.format.kind == 'csv'
    assert ds.format.sep == '\t'
