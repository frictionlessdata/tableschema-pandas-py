# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import io
import json
import pytest
import pandas as pd
from tabulator import topen
from jsontableschema.model import SchemaModel
from jsontableschema_pandas import Storage


# Tests

def test_storage():

    # Get resources
    articles_schema = json.load(io.open('data/articles.json', encoding='utf-8'))
    comments_schema = json.load(io.open('data/comments.json', encoding='utf-8'))
    articles_data = topen('data/articles.csv', with_headers=True).read()
    comments_data = topen('data/comments.csv', with_headers=True).read()

    # Storage
    storage = Storage()

    # Create tables
    storage.create('articles', articles_schema)
    storage.create('comments', comments_schema)

    assert storage['articles'].shape == (0, 0)
    assert storage['comments'].shape == (0, 0)

    # Write data to tables
    storage.write('articles', articles_data)
    storage.write('comments', comments_data)

    assert storage['articles'].shape == (2, 11)
    assert storage['comments'].shape == (1, 1)

    # Create existent table
    with pytest.raises(RuntimeError):
        storage.create('articles', articles_schema)

    # Get table representation
    assert repr(storage).startswith('Storage')

    # Get tables list
    assert storage.tables == ['articles', 'comments']

    # Get table schemas (takes schemas from cache)
    assert storage.describe('articles') == articles_schema
    assert storage.describe('comments') == comments_schema

    # Get table data
    assert list(storage.read('articles')) == convert_data(articles_schema, articles_data)
    assert list(storage.read('comments')) == convert_data(comments_schema, comments_data)

    # Delete tables
    for table in storage.tables:
        storage.delete(table)

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
    data = [(1, 'x'), (2, 'y')]

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
    storage = Storage(tables={'data': df})
    assert list(storage.read('data')) == [(1, 'a'), (2, 'b')]
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
    storage = Storage(tables={'data': df})
    assert list(storage.read('data')) == [(1, 'a'), (2, 'b')]
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
    assert str(excinfo.value) == 'Table "data" doesn\'t exist.'


def test_multiple_writes():
    index = pd.Index([1, 2], name='key')
    df = pd.DataFrame([('a',), ('b',)], columns=('value',), index=index)
    storage = Storage(tables={'data': df})
    storage.write('data', [(2, 'x'), (3, 'y')])
    assert list(storage.read('data')) == [
        (1, 'a'),
        (2, 'b'),
        (2, 'x'),
        (3, 'y'),
    ]


# Helpers

def convert_data(schema, data):
    model = SchemaModel(schema)
    return [tuple(model.convert_row(*item)) for item in data]
