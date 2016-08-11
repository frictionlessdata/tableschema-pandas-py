# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import datetime
import pandas as pd

from jsontableschema.model import SchemaModel
from jsontableschema_pandas.mappers import _convert_dtype, create_data_frame, restore_schema


def test_convert_dtype():
    df = pd.DataFrame([{
        'string': 'foo',
        'number': 3.14,
        'integer': 42,
        'boolean': True,
        'datetime': datetime.datetime.now(),
    }])

    assert _convert_dtype('string', df.dtypes['string']) == 'string'
    assert _convert_dtype('number', df.dtypes['number']) == 'number'
    assert _convert_dtype('integer', df.dtypes['integer']) == 'integer'
    assert _convert_dtype('boolean', df.dtypes['boolean']) == 'boolean'
    assert _convert_dtype('datetime', df.dtypes['datetime']) == 'datetime'

def test_create_data_frame_with_datetime_index():
    df = pd.read_csv("tests/vix.csv", sep=";", parse_dates=['Date'], index_col=['Date'])
    schema = restore_schema(df)
    model = SchemaModel(schema)
    data = df.reset_index().values
    df_new = create_data_frame(model, data)
    assert isinstance(df_new.index, pd.DatetimeIndex)

def test_create_data_frame():
    df = pd.read_csv("tests/sample.csv", sep=";", index_col=['Id'])
    schema = restore_schema(df)
    model = SchemaModel(schema)
    data = df.reset_index().values
    df_new = create_data_frame(model, data)
    assert isinstance(df_new.index, pd.Index)
