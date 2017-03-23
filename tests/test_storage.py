# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import datetime
import io
import json

import pandas as pd
import pytest
from jsontableschema import Schema
from jsontableschema_pandas import Storage
from tabulator import Stream


# Tests

def test_storage():
    # Get resources
    articles_descriptor = json.load(
        io.open('data/articles.json', encoding='utf-8'))
    comments_descriptor = json.load(
        io.open('data/comments.json', encoding='utf-8'))
    articles_rows = Stream('data/articles.csv', headers=1).open().read()
    comments_rows = Stream('data/comments.csv', headers=1).open().read()

    # Storage
    storage = Storage()

    # Create buckets
    storage.create('articles', articles_descriptor)
    storage.create('comments', comments_descriptor)

    # Assert rows
    assert storage['articles'].shape == (0, 0)
    assert storage['comments'].shape == (0, 0)

    # Write rows
    storage.write('articles', articles_rows)
    storage.write('comments', comments_rows)

    # Assert rows
    assert storage['articles'].shape == (2, 11)
    assert storage['comments'].shape == (1, 1)

    # Create existent bucket
    with pytest.raises(RuntimeError):
        storage.create('articles', articles_descriptor)

    # Assert representation
    assert repr(storage).startswith('Storage')

    # Assert buckets
    assert storage.buckets == ['articles', 'comments']

    # Assert descriptors (from cache)
    assert storage.describe('articles') == articles_descriptor
    assert storage.describe('comments') == comments_descriptor

    # Assert rows
    assert (normalize_date_types(list(storage.read('articles'))) ==
            normalize_date_types(sync_rows(articles_descriptor,
                                           articles_rows)))
    assert list(storage.read('comments')) == sync_rows(
        comments_descriptor, comments_rows)

    # Describe bucket
    storage.describe('articles', articles_descriptor)

    # Assert descriptor
    assert storage.describe('articles') == articles_descriptor

    # Delete buckets
    storage.delete()

    # Delete non existent bucket
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
            {'name': 'key', 'type': 'integer',
             'constraints': {'required': True}},
            {'name': 'value', 'type': 'string',
             'constraints': {'required': True}},
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
            {'name': 'key', 'type': 'integer',
             'constraints': {'required': True}},
            {'name': 'value', 'type': 'string',
             'constraints': {'required': True}},
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
        cast_row = schema.cast_row(row)
        result.append(cast_row)
    return result


def normalize_date_types(rows):
    cast_rows = []
    for row in rows:
        cast_row = []
        for value in row:
            if isinstance(value, datetime.date):
                cast_row.append(datetime.datetime.combine(
                    value, datetime.datetime.min.time()))
            elif isinstance(value, pd.Timestamp):
                cast_row.append(value.to_datetime())
            else:
                cast_row.append(value)
        cast_rows.append(cast_row)

    return cast_rows
