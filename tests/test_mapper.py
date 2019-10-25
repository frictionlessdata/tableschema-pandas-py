# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import six
import pytest
import datetime
import tableschema
import numpy as np
import pandas as pd
from tableschema_pandas.mapper import Mapper


# Tests

def test_mapper_convert_descriptor_and_rows():
    mapper = Mapper()
    df = pd.read_csv('data/sample.csv', sep=';', index_col=['Id'])
    descriptor = mapper.restore_descriptor(df)
    rows = df.reset_index().values
    df_new = mapper.convert_descriptor_and_rows(descriptor, rows)
    assert isinstance(df_new.index, pd.Index)


@pytest.mark.skip
def test_mapper_convert_descriptor_and_rows_with_datetime_index():
    mapper = Mapper()
    df = pd.read_csv('data/vix.csv', sep=';', parse_dates=['Date'], index_col=['Date'])
    descriptor = mapper.restore_descriptor(df)
    rows = df.reset_index().values
    df_new = mapper.convert_descriptor_and_rows(descriptor, rows)
    assert isinstance(df_new.index, pd.DatetimeIndex)


def test_mapper_convert_type():
    mapper = Mapper()
    assert mapper.convert_type('string') == np.dtype('O')
    assert mapper.convert_type('year') == np.dtype(int)
    assert mapper.convert_type('yearmonth') == np.dtype(list)
    assert mapper.convert_type('duration') == np.dtype('O')
    with pytest.raises(tableschema.exceptions.StorageError):
        mapper.convert_type('non-existent')


def test_mapper_restore_descriptor():
    mapper = Mapper()
    df = pd.read_csv('data/sample.csv', sep=';', index_col=['Id'])
    descriptor = mapper.restore_descriptor(df)
    assert descriptor == {
        'fields': [
            {'name': 'Id', 'type': 'integer', 'constraints': {'required': True}},
            {'name': 'Col1', 'type': 'number'},
            {'name': 'Col2', 'type': 'number'},
            {'name': 'Col3', 'type': 'number'},
        ],
        'primaryKey': 'Id',
     }


def test_mapper_restore_type():
    mapper = Mapper()
    df = pd.DataFrame([{
        'string': 'foo',
        'number': 3.14,
        'integer': 42,
        'boolean': True,
        'datetime': datetime.datetime.now(),
    }])
    assert mapper.restore_type(df.dtypes['string']) == 'string'
    assert mapper.restore_type(df.dtypes['number']) == 'number'
    assert mapper.restore_type(df.dtypes['integer']) == 'integer'
    assert mapper.restore_type(df.dtypes['boolean']) == 'boolean'
    assert mapper.restore_type(df.dtypes['datetime']) == 'datetime'
