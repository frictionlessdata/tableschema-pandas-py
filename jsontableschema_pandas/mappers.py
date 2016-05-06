# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import json
import numpy as np
import pandas as pd

from jsontableschema.model import SchemaModel
from jsontableschema.exceptions import InvalidObjectType


# Public API

def create_empty_data_frame(schema):
    model = SchemaModel(schema)
    columns = _get_columns(model)
    return pd.DataFrame(columns=columns)


def create_data_frame(model, data):
    items = _iter_items(model, data)
    columns = _get_columns(model)
    return pd.DataFrame.from_items(items, orient='index', columns=columns)


def restore_schema(data_frame):
    schema = {}
    schema['fields'] = fields = []

    # Primary key
    if data_frame.index.name:
        field_type = _convert_dtype(data_frame.index.dtype)
        field = {'name': data_frame.index.name, 'type': field_type}
        fields.append(field)
        schema['primaryKey'] = data_frame.index.name

    # Fields
    for column, dtype in data_frame.dtypes.items():
        field_type = _convert_dtype(dtype)
        field = {'name': column, 'type': field_type}
        if data_frame[column].isnull().sum() == 0:
            field['constraints'] = {'required': True}
        fields.append(field)

    return schema


# Private

def _get_columns(model):
    return [
        field['name']
        for field in model.fields
        if model.primaryKey != field['name']
    ]


def _iter_items(model, data):
    [('A', [1, 2, 3]), ('B', [4, 5, 6])]

    # Process data
    for row in data:
        pkey = None
        rdata = []
        for index, field in enumerate(model.fields):
            value = row[index]
            try:
                value = model.cast(field['name'], value)
            except InvalidObjectType:
                value = json.loads(value)
            if field['name'] == model.primaryKey:
                pkey = value
            else:
                rdata.append(value)
        yield pkey, rdata


def _convert_dtype(column, dtype):
    mapping = {
        np.dtype('int64'): 'integer',
    }

    try:
        return mapping[dtype]
    except KeyError:
        raise TypeError('type "%s" of column "%s" is not supported' % (
            dtype, column
        ))
