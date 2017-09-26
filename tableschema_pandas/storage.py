# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import six
import collections
import pandas as pd
import tableschema
from . import mappers


# Module API

class Storage(object):

    # Public

    def __init__(self, dataframes=None):
        """https://github.com/frictionlessdata/tableschema-pandas-py#storage
        """
        self.__dataframes = dataframes or collections.OrderedDict()
        self.__descriptors = {}

    def __repr__(self):
        """https://github.com/frictionlessdata/tableschema-pandas-py#storage
        """
        return 'Storage'

    def __getitem__(self, key):
        """https://github.com/frictionlessdata/tableschema-pandas-py#storage
        """
        return self.__dataframes[key]

    @property
    def buckets(self):
        """https://github.com/frictionlessdata/tableschema-pandas-py#storage
        """
        return list(self.__dataframes.keys())

    def create(self, bucket, descriptor, force=False):
        """https://github.com/frictionlessdata/tableschema-pandas-py#storage
        """

        # Make lists
        buckets = bucket
        if isinstance(bucket, six.string_types):
            buckets = [bucket]
        descriptors = descriptor
        if isinstance(descriptor, dict):
            descriptors = [descriptor]

        # Check buckets for existence
        for bucket in buckets:
            if bucket in self.buckets:
                if not force:
                    raise RuntimeError('Bucket "%s" already exists' % bucket)
                self.delete(bucket)

        # Define dataframes
        for bucket, descriptor in zip(buckets, descriptors):
            tableschema.validate(descriptor)
            self.__descriptors[bucket] = descriptor
            self.__dataframes[bucket] = pd.DataFrame()

    def delete(self, bucket=None, ignore=False):
        """https://github.com/frictionlessdata/tableschema-pandas-py#storage
        """

        # Make lists
        buckets = bucket
        if isinstance(bucket, six.string_types):
            buckets = [bucket]
        elif bucket is None:
            buckets = reversed(self.buckets)

        # Iterate over buckets
        for bucket in buckets:

            # Non existent bucket
            if bucket not in self.buckets:
                if not ignore:
                    raise RuntimeError('Bucket "%s" doesn\'t exist' % bucket)
                return

            # Remove from descriptors
            if bucket in self.__descriptors:
                del self.__descriptors[bucket]

            # Remove from dataframes
            if bucket in self.__dataframes:
                del self.__dataframes[bucket]

    def describe(self, bucket, descriptor=None):
        """https://github.com/frictionlessdata/tableschema-pandas-py#storage
        """

        # Set descriptor
        if descriptor is not None:
            self.__descriptors[bucket] = descriptor

        # Get descriptor
        else:
            descriptor = self.__descriptors.get(bucket)
            if descriptor is None:
                dataframe = self.__dataframes[bucket]
                descriptor = mappers.dataframe_to_descriptor(dataframe)

        return descriptor

    def iter(self, bucket):
        """https://github.com/frictionlessdata/tableschema-pandas-py#storage
        """

        # Check existense
        if bucket not in self.buckets:
            raise RuntimeError('Bucket "%s" doesn\'t exist.' % bucket)

        # Prepare
        descriptor = self.describe(bucket)
        schema = tableschema.Schema(descriptor)

        # Yield rows
        for pk, row in self.__dataframes[bucket].iterrows():
            rdata = []
            for field in schema.fields:
                if schema.primary_key and schema.primary_key[0] == field.name:
                    if str(pk) == 'nan':
                        pk = None
                    if pk and field.type == 'integer':
                        pk = int(pk)
                    rdata.append(field.cast_value(pk))
                else:
                    value = row[field.name]
                    if str(value) == 'nan':
                        value = None
                    if value and field.type == 'integer':
                        value = int(value)
                    rdata.append(field.cast_value(value))
            yield rdata

    def read(self, bucket):
        """https://github.com/frictionlessdata/tableschema-pandas-py#storage
        """
        rows = list(self.iter(bucket))
        return rows

    def write(self, bucket, rows):
        """https://github.com/frictionlessdata/tableschema-pandas-py#storage
        """

        # Prepare
        descriptor = self.describe(bucket)
        new_data_frame = mappers.descriptor_and_rows_to_dataframe(descriptor, rows)

        # Just set new DataFrame if current is empty
        if self.__dataframes[bucket].size == 0:
            self.__dataframes[bucket] = new_data_frame

        # Append new data frame to the old one setting new data frame
        # containing data from both old and new data frames
        else:
            self.__dataframes[bucket] = pd.concat([
                self.__dataframes[bucket],
                new_data_frame,
            ])
