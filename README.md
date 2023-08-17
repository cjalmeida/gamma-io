# Gamma IO

![python](https://img.shields.io/badge/python-3.8%2B-blue)
![build](https://github.com/cjalmeida/gamma-io/actions/workflows/build-deploy.yaml/badge.svg)
![cov](https://img.shields.io/badge/coverage-96%25-green)

Extensible I/O, filesystems and dataset layer for data-science projects.

## Overview

"Gamma IO" provides an opinionated way to implement a "I/O and datasets" layer to avoid
cluttering your "business layer" data pipelines with infra-structured concerns. One of
the main goals is to provide a simple way to adapt this layer to your particular needs.

We take an **open trunk** approach to code: the underlying code should be clean, very
easy to understand and extend. You can use the provided code **as a library dependency**
(eg. `pip install gamma-io`) or **you can simply "vendor" the code base** by copying it
over to your project as a Python module and extend it yourself.

While we provide a decent amount of functionality (see below), we do not expect this
project to be able to read, write and manage data from every single possible data
source. Our main goal is to provide a **consistent and nice way to write glue code** for
the data storage layer.

## Features

-   Clean interface to read/write datasets, separating infra configuration from data
    transformations.
-   Nice integration with [`gamma-config`][gamma-config]. But you can easily bring your
    own configuration provider!
-   Support for Pandas, PyArrow and Polars dataframes.
-   Support for reading files from multiple filesystems via [fsspec][fsspec].
-   First party support for partitioned Parquet and Feather (Arrow) datasets.


## Getting started

Using pip and assuming you're using the optional `gamma-config` integration:

```bash
pip install gamma-io gamma-config
```

You can "scaffold" an initial configuration. In your project folder:

```bash
python -m gamma.config.scaffold
```

Remove the sample files, then create yourself a `config/20-datasets.yaml` file
with the contents:

```yaml
datasets:
  source:
    customers_1k:
      location: https://github.com/cjalmeida/gamma-io/raw/main/samples/customers-1000.zip
      format: csv
      compression: zip

    customers_1k_plain:
      location: https://github.com/cjalmeida/gamma-io/raw/main/samples/customers-1000.csv
      format: csv

  raw:
    customers:
      location: "file:///tmp/gamma-io/data/customers"
      format: parquet
      compression: snappy
      partition_by: [cluster]

```

The file above provide two "layers": a `source` layer containing HTTPS remote
`customers_1k` and `customers_1k_plain` datasets, and a `raw` layer, containing a
`customers` dataset partitioned by the `cluster` column.

In your code (or Jupyter Notebook) you can read these datasets as Pandas dataframe
easily:

```python
from gamma.io import read_pandas

df = read_pandas("source", "customers_1k")
```

All details about dataset format and storage infrastructure is not cluttering the
codebase. Now let's write the dataset as set of partitioned Parquet files:

```python
from gamma.io import read_pandas, write_pandas

# read it again
df = read_pandas("source", "customers_1k")

# some transformation: let's add the cluster column to the dataset
df["cluster"] = (df["Index"] % 3).astype(str)

# write to our dataset
write_pandas(df, "raw", "customers")
```

You can see it generated the Parquet structured partitioned in the "Hive" format:

```bash
$ tree /tmp/gamma-io/data

/tmp/gamma-io/data
└── customers
    ├── cluster=0
    │   └── part-0.parquet
    ├── cluster=1
    │   └── part-0.parquet
    └── cluster=2
        └── part-0.parquet

```

## Configuring the filesystem

In the example above, the `location` configuration key points to where we can find the
dataset. The underlying infrastructure is based on [fsspec][fsspec], so it supports
many [filesystem-like implementations out of the box]().

[gamma-config]: https://cjalmeida.github.io/gamma-config
[fsspec]: https://filesystem-spec.readthedocs.io/en/latest/
