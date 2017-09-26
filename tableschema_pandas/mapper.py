# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import six
import json
import tableschema
import numpy as np
import pandas as pd
import pandas.core.common as pdc


# Module API

class Mapper(object):

    # Public

    def convert_descriptor(self, descriptor, rows):
        """Convert descriptor to Pandas
        """

        # Prepare
        primary_key = None
        schema = tableschema.Schema(descriptor)
        if len(schema.primary_key) == 1:
            primary_key = schema.primary_key[0]
        elif len(schema.primary_key) > 1:
            raise RuntimeError('Multi-column primary keys are not supported')

        # Get data/index
        data_rows = []
        index_rows = []
        jtstypes_map = {}
        for row in rows:
            values = []
            index = None
            for field, value in zip(schema.fields, row):
                try:
                    if str(value) == 'nan':
                        value = None
                    if value and field.type == 'integer':
                        value = int(value)
                    value = field.cast_value(value)
                except tableschema.exceptions.CastError:
                    value = json.loads(value)
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

    def convert_type(self, jtstype):
        """Convert type to Pandas
        """

        # Mapping
        MAPPING = {
            'string': np.dtype('O'),
            'number': np.dtype(float),
            'integer': np.dtype(int),
            'boolean': np.dtype(bool),
            'array': np.dtype(list),
            'object': np.dtype(dict),
            'date': np.dtype('datetime64[ns]'),
            'time': np.dtype('O'),
            'datetime': np.dtype('datetime64[ns]'),
            'year': np.dtype(int),
            'yearmonth': np.dtype(int),
            'geopoint': np.dtype('O'),
            'geojson': np.dtype('O'),
            'duration': np.dtype('O'),
            'any': np.dtype('O'),
        }

        # Get type
        try:
            dtype = MAPPING[jtstype]
        except KeyError:
            raise TypeError('JTS type "%s" is not supported' % jtstype)

        return dtype

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
            field_type = self.restore_type(dtype)
            field = {'name': column, 'type': field_type}
            if dataframe[column].isnull().sum() == 0:
                field['constraints'] = {'required': True}
            fields.append(field)

        # Descriptor
        descriptor = {}
        descriptor['fields'] = fields
        if primary_key:
            descriptor['primaryKey'] = primary_key

        return descriptor

    def restore_type(self, dtype):
        """Restore type from Pandas
        """

        # Convert
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
