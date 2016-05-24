# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import six
import json
import math
import numpy as np
import pandas as pd
import pandas.core.common as pdc
from jsontableschema.exceptions import InvalidObjectType


# Public API

JTS_TO_DTYPE = {
    'string': np.dtype('O'),
    'number': np.dtype(float),
    'integer': np.dtype(int),
    'boolean': np.dtype(bool),
    'null': np.dtype(None),
    'array': np.dtype(list),
    'object': np.dtype(dict),
    'date': np.dtype('O'),
    'time': np.dtype('O'),
    'datetime': np.dtype('datetime64[ns]'),
    'geopoint': np.dtype('O'),
    'geojson': np.dtype('O'),
    'any': np.dtype('O'),
}


def create_data_frame(model, data):
    index, data, dtypes = _get_index_and_data(model, data)
    dtypes = _schema_to_dtypes(model, dtypes)
    data = np.array(data, dtype=dtypes)
    columns = _get_columns(model)
    if model.primaryKey:
        pkey = model.get_field(model.primaryKey)
        index_dtype = JTS_TO_DTYPE[pkey['type']]
        index = pd.Index(index, name=model.primaryKey, dtype=index_dtype)
        return pd.DataFrame(data, index=index, columns=columns)
    else:
        return pd.DataFrame(data, columns=columns)


def restore_schema(data_frame):
    schema = {}
    schema['fields'] = fields = []

    # Primary key
    if data_frame.index.name:
        field_type = _convert_dtype(
            data_frame.index.name, data_frame.index.dtype)
        field = {
            'name': data_frame.index.name,
            'type': field_type,
            'constraints': {'required': True},
        }
        fields.append(field)
        schema['primaryKey'] = data_frame.index.name

    # Fields
    for column, dtype in data_frame.dtypes.iteritems():
        field_type = _convert_dtype(column, dtype)
        field = {'name': column, 'type': field_type}
        if data_frame[column].isnull().sum() == 0:
            field['constraints'] = {'required': True}
        fields.append(field)

    return schema


def pandas_dtype_to_python(value):
    """Converts Pandas data types to python objects
    """
    if isinstance(value, float) and math.isnan(value):
        return None
    elif isinstance(value, pd.Timestamp):
        return value.to_datetime()
    # TODO: I guess there are more types to convert, could not find a canonical
    #       list of scalar Pandas data types, but using following command:
    #
    #           [x for x in dir(pd)
    #            if x[0].isupper() and not hasattr(getattr(pd, x), '__len__')]
    #
    #       I found these types:
    #
    #           DateOffset, NaT, Period, Timedelta, Timestamp
    else:
        return value


# Private

def _get_columns(model):
    return [
        field['name']
        for field in model.fields
        if model.primaryKey != field['name']
    ]


def _get_index_and_data(model, rows):
    index = []
    data = []
    dtypes = {}
    for row in rows:
        pkey = None
        rdata = []
        for i, field in enumerate(model.fields):
            value = row[i]
            try:
                value = model.cast(field['name'], value)
            except InvalidObjectType:
                value = json.loads(value)
            if value is None and field['type'] in ('number', 'integer'):
                dtypes[field['name']] = JTS_TO_DTYPE['number']
                value = np.NaN
            if field['name'] == model.primaryKey:
                pkey = value
            else:
                rdata.append(value)
        index.append(pkey)
        data.append(tuple(rdata))
    return index, data, dtypes


def _convert_dtype(column, dtype):
    if pdc.is_bool_dtype(dtype):
        return 'boolean'
    elif pdc.is_integer_dtype(dtype):
        return 'integer'
    elif pdc.is_numeric_dtype(dtype):
        return 'number'
    elif pdc.is_datetime64_any_dtype(dtype):
        return 'datetime'
    else:
        return 'string'


def _schema_to_dtypes(model, overrides=None):
    overrides = overrides or {}
    dtypes = []
    for index, field in enumerate(model.fields):
        if field['name'] != model.primaryKey:
            dtype = overrides.get(field['name'], JTS_TO_DTYPE[field['type']])
            if six.PY2:  # pragma: no cover
                dtypes.append((field['name'].encode('utf-8'), dtype))
            else:
                dtypes.append((field['name'], dtype))
    return dtypes
