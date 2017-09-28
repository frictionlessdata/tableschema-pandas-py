# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import io
import json
import pytest
import datetime
import tableschema
import pandas as pd
from copy import deepcopy
from decimal import Decimal
from tabulator import Stream
from collections import OrderedDict
from tableschema_pandas import Storage


# Resources

ARTICLES = {
    'schema': {
        'fields': [
            {'name': 'id', 'type': 'integer', 'constraints': {'required': True}},
            {'name': 'parent', 'type': 'integer'},
            {'name': 'name', 'type': 'string'},
            {'name': 'current', 'type': 'boolean'},
            {'name': 'rating', 'type': 'number'},
        ],
        'primaryKey': 'id',
        # 'foreignKeys': [
            # {'fields': 'parent', 'reference': {'resource': '', 'fields': 'id'}},
        # ],
    },
    'data': [
        ['1', '', 'Taxes', 'True', '9.5'],
        ['2', '1', '中国人', 'False', '7'],
    ],
}
COMMENTS = {
    'schema': {
        'fields': [
            {'name': 'entry_id', 'type': 'integer', 'constraints': {'required': True}},
            {'name': 'comment', 'type': 'string'},
            {'name': 'note', 'type': 'any'},
        ],
        'primaryKey': 'entry_id',
        # 'foreignKeys': [
            # {'fields': 'entry_id', 'reference': {'resource': 'articles', 'fields': 'id'}},
        # ],
    },
    'data': [
        ['1', 'good', 'note1'],
        ['2', 'bad', 'note2'],
    ],
}
TEMPORAL = {
    'schema': {
        'fields': [
            {'name': 'date', 'type': 'date'},
            {'name': 'date_year', 'type': 'date', 'format': '%Y'},
            {'name': 'datetime', 'type': 'datetime'},
            {'name': 'duration', 'type': 'duration'},
            {'name': 'time', 'type': 'time'},
            {'name': 'year', 'type': 'year'},
            {'name': 'yearmonth', 'type': 'yearmonth'},
        ],
    },
    'data': [
        ['2015-01-01', '2015', '2015-01-01T03:00:00Z', 'P1Y1M', '03:00:00', '2015', '2015-01'],
        ['2015-12-31', '2015', '2015-12-31T15:45:33Z', 'P2Y2M', '15:45:33', '2015', '2015-01'],
    ],
}
LOCATION = {
    'schema': {
        'fields': [
            {'name': 'location', 'type': 'geojson'},
            {'name': 'geopoint', 'type': 'geopoint'},
        ],
    },
    'data': [
        ['{"type": "Point","coordinates":[33.33,33.33]}', '30,75'],
        ['{"type": "Point","coordinates":[50.00,50.00]}', '90,45'],
    ],
}
COMPOUND = {
    'schema': {
        'fields': [
            {'name': 'stats', 'type': 'object'},
            {'name': 'persons', 'type': 'array'},
        ],
    },
    'data': [
        ['{"chars":560}', '["Mike", "John"]'],
        ['{"chars":970}', '["Paul", "Alex"]'],
    ],
}


# Tests

def test_storage():

    # Create storage
    storage = Storage()

    # Delete buckets
    storage.delete()

    # Create buckets
    storage.create(['articles', 'comments'], [ARTICLES['schema'], COMMENTS['schema']])
    storage.create('comments', COMMENTS['schema'], force=True)
    storage.create('temporal', TEMPORAL['schema'])
    storage.create('location', LOCATION['schema'])
    storage.create('compound', COMPOUND['schema'])

    # Write data
    storage.write('articles', ARTICLES['data'])
    storage.write('comments', COMMENTS['data'])
    storage.write('temporal', TEMPORAL['data'])
    storage.write('location', LOCATION['data'])
    storage.write('compound', COMPOUND['data'])

    # Create new storage to use reflection only
    dataframes = OrderedDict()
    dataframes['articles'] = storage['articles']
    dataframes['comments'] = storage['comments']
    dataframes['temporal'] = storage['temporal']
    dataframes['location'] = storage['location']
    dataframes['compound'] = storage['compound']
    storage = Storage(dataframes=dataframes)

    # Create existent bucket
    with pytest.raises(tableschema.exceptions.StorageError):
        storage.create('articles', ARTICLES['schema'])

    # Assert buckets
    assert storage.buckets == ['articles', 'comments', 'compound', 'location', 'temporal']

    # Assert schemas
    assert storage.describe('articles') == {
        'fields': [
            {'name': 'id', 'type': 'integer', 'constraints': {'required': True}},
            {'name': 'parent', 'type': 'number'}, # type downgrade
            {'name': 'name', 'type': 'string'},
            {'name': 'current', 'type': 'boolean'},
            {'name': 'rating', 'type': 'number'},
        ],
        'primaryKey': 'id',
    }
    assert storage.describe('comments') == {
        'fields': [
            {'name': 'entry_id', 'type': 'integer', 'constraints': {'required': True}},
            {'name': 'comment', 'type': 'string'},
            {'name': 'note', 'type': 'string'}, # type downgrade
        ],
        'primaryKey': 'entry_id',
    }
    assert storage.describe('temporal') == {
        'fields': [
            {'name': 'date', 'type': 'date'},
            {'name': 'date_year', 'type': 'date'}, # format removal
            {'name': 'datetime', 'type': 'datetime'},
            {'name': 'duration', 'type': 'duration'},
            {'name': 'time', 'type': 'time'},
            {'name': 'year', 'type': 'integer'}, # type downgrade
            {'name': 'yearmonth', 'type': 'array'}, # type downgrade
        ],
    }
    assert storage.describe('location') == {
        'fields': [
            {'name': 'location', 'type': 'object'}, # type downgrade
            {'name': 'geopoint', 'type': 'array'}, # type downgrade
        ],
    }
    assert storage.describe('compound') == COMPOUND['schema']

    assert storage.read('articles') == cast(ARTICLES)['data']
    assert storage.read('comments') == cast(COMMENTS)['data']
    assert storage.read('temporal') == cast(TEMPORAL, wrap={'yearmonth': list})['data']
    assert storage.read('location') == cast(LOCATION, wrap_each={'geopoint': Decimal})['data']
    assert storage.read('compound') == cast(COMPOUND)['data']

    # Assert data with forced schema
    storage.describe('compound', COMPOUND['schema'])
    assert storage.read('compound') == cast(COMPOUND)['data']

    # Delete non existent bucket
    with pytest.raises(tableschema.exceptions.StorageError):
        storage.delete('non_existent')

    # Delete buckets
    storage.delete()


def test_storage_table_without_primary_key():
    schema = {
        'fields': [
            {'name': 'a', 'type': 'integer'},
            {'name': 'b', 'type': 'string'},
        ]
    }
    data = [[1, 'x'], [2, 'y']]

    storage = Storage()
    storage.create('data', schema)
    storage.write('data', data)
    assert list(storage.read('data')) == data


def test_storage_init_tables():
    data = [
        (1, 'a'),
        (2, 'b'),
    ]
    df = pd.DataFrame(data, columns=('key', 'value'))
    storage = Storage(dataframes={'data': df})
    assert list(storage.read('data')) == [[1, 'a'], [2, 'b']]
    assert storage.describe('data') == {
        'fields': [
            {'name': 'key', 'type': 'integer'},
            {'name': 'value', 'type': 'string'},
        ]
    }


def test_storage_restore_schema_with_primary_key():
    data = [
        ('a',),
        ('b',),
    ]
    index = pd.Index([1, 2], name='key')
    df = pd.DataFrame(data, columns=('value',), index=index)
    storage = Storage(dataframes={'data': df})
    assert list(storage.read('data')) == [[1, 'a'], [2, 'b']]
    assert storage.describe('data') == {
        'primaryKey': 'key',
        'fields': [
            {'name': 'key', 'type': 'integer', 'constraints': {'required': True}},
            {'name': 'value', 'type': 'string'},
        ]
    }


def test_storage_read_missing_table():
    storage = Storage()
    with pytest.raises(tableschema.exceptions.StorageError) as excinfo:
        list(storage.read('data'))
    assert str(excinfo.value) == 'Bucket "data" doesn\'t exist.'


def test_storage_multiple_writes():
    index = pd.Index([1, 2], name='key')
    df = pd.DataFrame([('a',), ('b',)], columns=('value',), index=index)
    storage = Storage(dataframes={'data': df})
    storage.write('data', [(2, 'x'), (3, 'y')])
    assert list(storage.read('data')) == [
        [1, 'a'],
        [2, 'b'],
        [2, 'x'],
        [3, 'y'],
    ]


# Helpers

def cast(resource, skip=[], wrap={}, wrap_each={}):
    resource = deepcopy(resource)
    schema = tableschema.Schema(resource['schema'])
    for row in resource['data']:
        for index, field in enumerate(schema.fields):
            value = row[index]
            if field.type not in skip:
                value = field.cast_value(value)
            if field.type in wrap:
                value = wrap[field.type](value)
            if field.type in wrap_each:
                value = list(map(wrap_each[field.type], value))
            row[index] = value
    return resource
