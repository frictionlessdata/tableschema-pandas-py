# tableschema-pandas-py

[![Travis](https://img.shields.io/travis/frictionlessdata/tableschema-pandas-py/master.svg)](https://travis-ci.org/frictionlessdata/tableschema-pandas-py)
[![Coveralls](http://img.shields.io/coveralls/frictionlessdata/tableschema-pandas-py.svg?branch=master)](https://coveralls.io/r/frictionlessdata/tableschema-pandas-py?branch=master)
[![PyPi](https://img.shields.io/pypi/v/tableschema-pandas.svg)](https://pypi.python.org/pypi/tableschema-pandas)
[![Github](https://img.shields.io/badge/github-master-brightgreen)](https://github.com/frictionlessdata/tableschema-pandas-py)
[![Gitter](https://img.shields.io/gitter/room/frictionlessdata/chat.svg)](https://gitter.im/frictionlessdata/chat)

Generate and load Pandas data frames [Table Schema](http://specs.frictionlessdata.io/table-schema/) descriptors.

## Features

- implements `tableschema.Storage` interface

## Contents

<!--TOC-->

  - [Getting Started](#getting-started)
    - [Installation](#installation)
  - [Documentation](#documentation)
  - [API Reference](#api-reference)
    - [`Storage`](#storage)
  - [Contributing](#contributing)
  - [Changelog](#changelog)

<!--TOC-->

## Getting Started

### Installation

The package use semantic versioning. It means that major versions  could include breaking changes. It's highly recommended to specify `package` version range in your `setup/requirements` file e.g. `package>=1.0,<2.0`.

```
$ pip install tableschema-pandas
```

## Documentation

```python
# pip install datapackage tableschema-pandas
from datapackage import Package

# Save to Pandas

package = Package('http://data.okfn.org/data/core/country-list/datapackage.json')
storage = package.save(storage='pandas')

print(type(storage['data']))
#  <class 'pandas.core.frame.DataFrame'>

print(storage['data'].head())
#               Name   Code
#  0     Afghanistan   AF
#  1   Åland Islands   AX
#  2         Albania   AL
#  3         Algeria   DZ
#  4  American Samoa   AS

# Load from Pandas

package = Package(storage=storage)
print(package.descriptor)
print(package.resources[0].read())
```

Storage works as a container for Pandas data frames. You can define new data frame inside storage using `storage.create` method:

```python
>>> from tableschema_pandas import Storage

>>> storage = Storage()
```

```python
>>> storage.create('data', {
...     'primaryKey': 'id',
...     'fields': [
...         {'name': 'id', 'type': 'integer'},
...         {'name': 'comment', 'type': 'string'},
...     ]
... })

>>> storage.buckets
['data']

>>> storage['data'].shape
(0, 0)
```

Use `storage.write` to populate data frame with data:

```python
>>> storage.write('data', [(1, 'a'), (2, 'b')])

>>> storage['data']
id comment
1        a
2        b
```

Also you can use [tabulator](https://github.com/frictionlessdata/tabulator-py) to populate data frame from external data file. As you see, subsequent writes simply appends new data on top of existing ones:

```python
>>> import tabulator

>>> with tabulator.Stream('data/comments.csv', headers=1) as stream:
...     storage.write('data', stream)

>>> storage['data']
id comment
1        a
2        b
1     good
```

## API Reference

### `Storage`
```python
Storage(self, dataframes=None)
```
Pandas storage

Package implements
[Tabular Storage](https://github.com/frictionlessdata/tableschema-py#storage)
interface (see full documentation on the link):

![Storage](https://i.imgur.com/RQgrxqp.png)

> Only additional API is documented

__Arguments__
- __dataframes (object[])__: list of storage dataframes


## Contributing

> The project follows the [Open Knowledge International coding standards](https://github.com/okfn/coding-standards).

Recommended way to get started is to create and activate a project virtual environment.
To install package and development dependencies into active environment:

```bash
$ make install
```

To run tests with linting and coverage:

```bash
$ make test
```

## Changelog

Here described only breaking and the most important changes. The full changelog and documentation for all released versions could be found in nicely formatted [commit history](https://github.com/frictionlessdata/tableschema-pandas-py/commits/master).

#### v1.1

- Added support for composite primary keys (loading to pandas)

#### v1.0

- Initial driver implementation
