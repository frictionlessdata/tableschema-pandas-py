# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import six
import json
import isodate
import datetime
import tableschema
import numpy as np
import pandas as pd

# Starting from pandas@0.24 there is the new API
# https://github.com/frictionlessdata/tableschema-pandas-py/issues/29
try:
    import pandas.core.dtypes.api as pdc
except ImportError:
    import pandas.core.common as pdc


# Module API

class Mapper(object):

    # Public

    def convert_descriptor_and_rows(self, descriptor, rows):
        """Convert descriptor and rows to Pandas
        """

        # Prepare
        primary_key = None
        schema = tableschema.Schema(descriptor)
        if len(schema.primary_key) == 1:
            primary_key = schema.primary_key[0]
        elif len(schema.primary_key) > 1:
            message = 'Multi-column primary keys are not supported'
            raise tableschema.exceptions.StorageError(message)

        # Get data/index
        data_rows = []
        index_rows = []
        jtstypes_map = {}
        for row in rows:
            values = []
            index = None
            for field, value in zip(schema.fields, row):
                try:
                    if isinstance(value, float) and np.isnan(value):
                        value = None
                    if value and field.type == 'integer':
                        value = int(value)
                    value = field.cast_value(value)
                except tableschema.exceptions.CastError:
                    value = json.loads(value)
                # http://pandas.pydata.org/pandas-docs/stable/gotchas.html#support-for-integer-na
                if value is None and field.type in ('number', 'integer'):
                    jtstypes_map[field.name] = 'number'
                    value = np.NaN
                if field.name == primary_key:
                    index = value
                else:
                    values.append(value)
            data_rows.append(tuple(values))
            index_rows.append(index)

        # Get dtypes
        dtypes = []
        for field in schema.fields:
            if field.name != primary_key:
                field_name = field.name
                if six.PY2:
                    field_name = field.name.encode('utf-8')
                dtype = self.convert_type(jtstypes_map.get(field.name, field.type))
                dtypes.append((field_name, dtype))

        # Create dataframe
        index = None
        columns = schema.headers
        array = np.array(data_rows, dtype=dtypes)
        if primary_key:
            index_field = schema.get_field(primary_key)
            index_dtype = self.convert_type(index_field.type)
            index_class = pd.Index
            if index_field.type in ['datetime', 'date']:
                index_class = pd.DatetimeIndex
            index = index_class(index_rows, name=primary_key, dtype=index_dtype)
            columns = filter(lambda column: column != primary_key, schema.headers)
        dataframe = pd.DataFrame(array, index=index, columns=columns)

        return dataframe

    def convert_type(self, type):
        """Convert type to Pandas
        """

        # Mapping
        mapping = {
            'any': np.dtype('O'),
            'array': np.dtype(list),
            'boolean': np.dtype(bool),
            'date': np.dtype('O'),
            'datetime': np.dtype('datetime64[ns]'),
            'duration': np.dtype('O'),
            'geojson': np.dtype('O'),
            'geopoint': np.dtype('O'),
            'integer': np.dtype(int),
            'number': np.dtype(float),
            'object': np.dtype(dict),
            'string': np.dtype('O'),
            'time': np.dtype('O'),
            'year': np.dtype(int),
            'yearmonth': np.dtype('O'),
        }

        # Get type
        if type not in mapping:
            message = 'Type "%s" is not supported' % type
            raise tableschema.exceptions.StorageError(message)

        return mapping[type]

    def restore_descriptor(self, dataframe):
        """Restore descriptor from Pandas
        """

        # Prepare
        fields = []
        primary_key = None

        # Primary key
        if dataframe.index.name:
            field_type = self.restore_type(dataframe.index.dtype)
            field = {
                'name': dataframe.index.name,
                'type': field_type,
                'constraints': {'required': True},
            }
            fields.append(field)
            primary_key = dataframe.index.name

        # Fields
        for column, dtype in dataframe.dtypes.iteritems():
            sample = dataframe[column].iloc[0] if len(dataframe) else None
            field_type = self.restore_type(dtype, sample=sample)
            field = {'name': column, 'type': field_type}
            # TODO: provide better required indication
            # if dataframe[column].isnull().sum() == 0:
            #     field['constraints'] = {'required': True}
            fields.append(field)

        # Descriptor
        descriptor = {}
        descriptor['fields'] = fields
        if primary_key:
            descriptor['primaryKey'] = primary_key

        return descriptor

    def restore_row(self, row, schema, pk):
        """Restore row from Pandas
        """
        result = []
        for field in schema.fields:
            if schema.primary_key and schema.primary_key[0] == field.name:
                if field.type == 'number' and np.isnan(pk):
                    pk = None
                if pk and field.type == 'integer':
                    pk = int(pk)
                result.append(field.cast_value(pk))
            else:
                value = row[field.name]
                if field.type == 'number' and np.isnan(value):
                    value = None
                if value and field.type == 'integer':
                    value = int(value)
                elif field.type == 'datetime':
                    value = value.to_pydatetime()
                result.append(field.cast_value(value))
        return result

    def restore_type(self, dtype, sample=None):
        """Restore type from Pandas
        """

        # Pandas types
        if pdc.is_bool_dtype(dtype):
            return 'boolean'
        elif pdc.is_datetime64_any_dtype(dtype):
            return 'datetime'
        elif pdc.is_integer_dtype(dtype):
            return 'integer'
        elif pdc.is_numeric_dtype(dtype):
            return 'number'

        # Python types
        if sample is not None:
            if isinstance(sample, (list, tuple)):
                return 'array'
            elif isinstance(sample, datetime.date):
                return 'date'
            elif isinstance(sample, isodate.Duration):
                return 'duration'
            elif isinstance(sample, dict):
                return 'object'
            elif isinstance(sample, six.string_types):
                return 'string'
            elif isinstance(sample, datetime.time):
                return 'time'

        return 'string'
