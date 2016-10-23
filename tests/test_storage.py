# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import io
import json
import pytest
import pandas as pd
from tabulator import Stream
from jsontableschema import Schema
from jsontableschema_pandas import Storage


# Tests

def test_storage():

    # Get resources
    articles_descriptor = json.load(io.open('data/articles.json', encoding='utf-8'))
    comments_descriptor = json.load(io.open('data/comments.json', encoding='utf-8'))
    articles_rows = Stream('data/articles.csv', headers=1).open().read()
    comments_rows = Stream('data/comments.csv', headers=1).open().read()

    # Storage
    storage = Storage()

    # Create tables
    storage.create('articles', articles_descriptor)
    storage.create('comments', comments_descriptor)

    assert storage['articles'].shape == (0, 0)
    assert storage['comments'].shape == (0, 0)

    # Write data to tables
    storage.write('articles', articles_rows)
    storage.write('comments', comments_rows)

    assert storage['articles'].shape == (2, 11)
    assert storage['comments'].shape == (1, 1)

    # Create existent table
    with pytest.raises(RuntimeError):
        storage.create('articles', articles_descriptor)

    # Get table representation
    assert repr(storage).startswith('Storage')

    # Get tables list
    assert storage.buckets == ['articles', 'comments']

    # Get table schemas (takes schemas from cache)
    assert storage.describe('articles') == articles_descriptor
    assert storage.describe('comments') == comments_descriptor

    # Get table data
    assert list(storage.read('articles')) == sync_rows(articles_descriptor, articles_rows)
    assert list(storage.read('comments')) == sync_rows(comments_descriptor, comments_rows)

    # Delete tables
    for bucket in storage.buckets:
        storage.delete(bucket)

    # Delete non existent table
    with pytest.raises(RuntimeError):
        storage.delete('articles')


def test_table_without_primary_key():
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


def test_init_tables():
    data = [
        (1, 'a'),
        (2, 'b'),
    ]
    df = pd.DataFrame(data, columns=('key', 'value'))
    storage = Storage(dataframes={'data': df})
    assert list(storage.read('data')) == [[1, 'a'], [2, 'b']]
    assert storage.describe('data') == {
        'fields': [
            {'name': 'key', 'type': 'integer', 'constraints': {'required': True}},
            {'name': 'value', 'type': 'string', 'constraints': {'required': True}},
        ]
    }


def test_restore_schema_with_primary_key():
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
            {'name': 'value', 'type': 'string', 'constraints': {'required': True}},
        ]
    }


def test_read_missing_table():
    storage = Storage()
    with pytest.raises(RuntimeError) as excinfo:
        list(storage.read('data'))
    assert str(excinfo.value) == 'Bucket "data" doesn\'t exist.'


def test_multiple_writes():
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

def sync_rows(descriptor, rows):
    result = []
    schema = Schema(descriptor)
    for row in rows:
        result.append(schema.cast_row(row))
    return result
