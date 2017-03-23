# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest
import datetime
import numpy as np
import pandas as pd
from jsontableschema import Schema
from jsontableschema_pandas import mappers


def test_dataframe_to_descriptor():
    df = pd.read_csv('data/sample.csv', sep=';', index_col=['Id'])
    descriptor = mappers.dataframe_to_descriptor(df)
    assert descriptor == {
        'fields': [
            {'name': 'Id', 'constraints': {'required': True}, 'type': 'integer'},
            {'name': 'Col1', 'constraints': {'required': True}, 'type': 'number'},
            {'name': 'Col2', 'constraints': {'required': True}, 'type': 'number'},
            {'name': 'Col3', 'constraints': {'required': True}, 'type': 'number'},
        ],
        'primaryKey': 'Id',
     }


def test_descriptor_and_rows_to_dataframe():
    df = pd.read_csv('data/sample.csv', sep=';', index_col=['Id'])
    descriptor = mappers.dataframe_to_descriptor(df)
    rows = df.reset_index().values
    df_new = mappers.descriptor_and_rows_to_dataframe(descriptor, rows)
    assert isinstance(df_new.index, pd.Index)


def test_descriptor_and_rows_to_dataframe_with_datetime_index():
    df = pd.read_csv('data/vix.csv', sep=';', parse_dates=['Date'], index_col=['Date'])
    descriptor = mappers.dataframe_to_descriptor(df)
    rows = df.reset_index().values
    df_new = mappers.descriptor_and_rows_to_dataframe(descriptor, rows)
    assert isinstance(df_new.index, pd.DatetimeIndex)


def test_descriptor_and_rows_to_dataframe_composite_primary_key_not_supported():
    descriptor = {'fields': [{'name': 'a'}, {'name': 'b'}], 'primaryKey': ['a', 'b']}
    with pytest.raises(RuntimeError):
        mappers.descriptor_and_rows_to_dataframe(descriptor, [])


def test_jtstype_to_dtype():
    assert mappers.jtstype_to_dtype('string') == np.dtype('O')
    assert mappers.jtstype_to_dtype('year') == np.dtype(int)
    assert mappers.jtstype_to_dtype('yearmonth') == np.dtype(int)
    assert mappers.jtstype_to_dtype('duration') == np.dtype('O')
    with pytest.raises(TypeError):
        mappers.jtstype_to_dtype('non-existent')


def test_dtype_to_jtstype():
    df = pd.DataFrame([{
        'string': 'foo',
        'number': 3.14,
        'integer': 42,
        'boolean': True,
        'datetime': datetime.datetime.now(),
    }])
    assert mappers.dtype_to_jtstype(df.dtypes['string']) == 'string'
    assert mappers.dtype_to_jtstype(df.dtypes['number']) == 'number'
    assert mappers.dtype_to_jtstype(df.dtypes['integer']) == 'integer'
    assert mappers.dtype_to_jtstype(df.dtypes['boolean']) == 'boolean'
    assert mappers.dtype_to_jtstype(df.dtypes['datetime']) == 'datetime'
