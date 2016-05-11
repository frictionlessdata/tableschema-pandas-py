# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import six
import collections
import pandas as pd

import jsontableschema
from jsontableschema import storage as base
from jsontableschema.model import SchemaModel

from . import mappers
from .utils import force_list


# Module API

class Storage(base.Storage):

    # Public

    def __init__(self, prefix='', tables=None):

        # Set attributes
        self.__prefix = prefix
        self.__tables = tables or collections.OrderedDict()
        self.__schemas = {}

    def __repr__(self):
        return 'Storage'

    def __getitem__(self, key):
        return self.__tables[key]

    @property
    def tables(self):
        return list(self.__tables.keys())

    def check(self, table):
        return table in self.__tables

    def create(self, table, schema):
        # Make lists
        tables = force_list(table, six.string_types)
        schemas = force_list(schema, dict)
        assert len(tables) == len(schemas)

        # Check tables for existence
        for table in tables:
            if self.check(table):
                raise RuntimeError('Table "%s" already exists.' % table)

        # Define tables
        for table, schema in zip(tables, schemas):

            # Add to schemas
            jsontableschema.validate(schema)
            self.__schemas[table] = schema

            # Create Pandas DataFrame
            self.__tables[table] = pd.DataFrame()

    def delete(self, table):
        tables = force_list(table, six.string_types)
        for table in tables:
            # Check existent
            if not self.check(table):
                raise RuntimeError('Table "%s" doesn\'t exist.' % table)

            # Remove from schemas
            if table in self.__schemas:
                del self.__schemas[table]

            # Remove from tables
            if table in self.__tables:
                del self.__tables[table]

    def describe(self, table):
        if table in self.__schemas:
            return self.__schemas[table]
        else:
            return mappers.restore_schema(self.__tables[table])

    def read(self, table):
        if not self.check(table):
            raise RuntimeError('Table "%s" doesn\'t exist.' % table)

        schema = self.describe(table)
        model = SchemaModel(schema)

        for pk, row in self.__tables[table].iterrows():
            rdata = []
            for field in model.fields:
                name = field['name']
                if name == model.primaryKey:
                    rdata.append(mappers.pandas_dtype_to_python(pk))
                else:
                    rdata.append(mappers.pandas_dtype_to_python(row[name]))
            yield tuple(rdata)

    def write(self, table, data):
        schema = self.describe(table)
        model = SchemaModel(schema)
        new_data_frame = mappers.create_data_frame(model, data)

        # Just return new DataFrame if current is empty
        if self.__tables[table].size == 0:
            self.__tables[table] = new_data_frame

        # Append new data frame to the old one returning new data frame
        # containing data from both old and new data frames
        else:
            self.__tables[table] = pd.concat([
                self.__tables[table],
                new_data_frame
            ])
