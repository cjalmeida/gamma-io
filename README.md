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
foo: 1
user: !env USER
```

To access the config from within your Python program:

```python
import os
from gamma.config import get_config

def run():

    # it's safe and efficient to call this multiple times
    config = get_config()

    # get static value using the dict keys or attribute access
    assert config["foo"] == 1
    assert config.foo == 1

    # get dynamic variables
    assert config["user"] == os.getenv("USER")
    assert config.user == os.getenv("USER")
```

Most of the magic happen via tags. Look at the documentation for info on the [built-in tags](tags) available.

## Changelog

### Breaking in 0.7

-   We've **DEPRECATED** our homegrown multiple dispatch system `gamma.dispatch`,
    replacing it by [`plum`][plum]. Unless you were using `gamma.dispatch` directly, or
    extending via [custom tags][custom tags], no need to worry. The `gamma.dispatch`
    package will be removed by release 1.0.

### New in 0.7

-   We have a new home in https://github.com/cjalmeida/gamma-config !

### Breaking in 0.6

-   Strict support for [YAML 1.2 Core Schema](https://yaml.org/spec/1.2.1/#id2804923).
    In practice, unquoted ISO8610 dates (eg. `2022-12-20`) won't get converted
    to `datetime.date` or `datetime.datetime` objects. Use `!date` or `!datetime`
    if needed.
-   `.env` files are loaded automatically and get precedence over `config.env`
    and `config.local.env`.
-   Use of `config.env` and `config.local.env` is deprecated.
-   Default scaffolded `include_folder` interpret `ENVIRONMENT` variable string like
    `foo bar` as two separate environment subfolders.
-   (dispatch) `Val` arguments passed as class (eg. `foo(Val['bar'])`) will be converted
    to instance, as if it were called `foo(Val['bar']())`
-   The `!py:<module>:<func>` will no longer a single `None` argument

### New in 0.6

-   Support for [YAML Anchors and Aliases](https://www.educative.io/blog/advanced-yaml-syntax-cheatsheet#anchors)

### Breaking changes in 0.5

-   When using the dot (`.`) syntax, missing values raise `AttributeError` instead of returning
    a false-y object.
-   Dropped support for Python 3.7

### New in 0.5

-   We're now in PyPI!
-   Options for installing extra dependencies (eg. `jinja2`, `pydantic`)

[gamma-config]: https://cjalmeida.github.io/gamma-config
[fsspec]: https://filesystem-spec.readthedocs.io/en/latest/
