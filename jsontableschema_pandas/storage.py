# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import six
import collections
import pandas as pd
import jsontableschema
from jsontableschema import Schema
from . import mappers


# Module API

class Storage(object):
    """Pandas Tabular Storage.

    It's an implementation of `jsontablescema.Storage`.

    Args:
        dataframes (list): list of storage dataframes

    """

    # Public

    def __init__(self, dataframes=None):
        self.__dataframes = dataframes or collections.OrderedDict()
        self.__descriptors = {}

    def __repr__(self):
        return 'Storage'

    def __getitem__(self, key):
        return self.__dataframes[key]

    @property
    def buckets(self):
        return list(self.__dataframes.keys())

    def create(self, bucket, descriptor, force=False):

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
            jsontableschema.validate(descriptor)
            self.__descriptors[bucket] = descriptor
            self.__dataframes[bucket] = pd.DataFrame()

    def delete(self, bucket=None, ignore=False):

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

            # Remove from descriptors
            if bucket in self.__descriptors:
                del self.__descriptors[bucket]

            # Remove from dataframes
            if bucket in self.__dataframes:
                del self.__dataframes[bucket]

    def describe(self, bucket, descriptor=None):

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

        # Check existense
        if bucket not in self.buckets:
            raise RuntimeError('Bucket "%s" doesn\'t exist.' % bucket)

        # Prepare
        descriptor = self.describe(bucket)
        schema = Schema(descriptor)

        # Yield rows
        for pk, row in self.__dataframes[bucket].iterrows():
            rdata = []
            for field in schema.fields:
                if schema.primary_key and schema.primary_key[0] == field.name:
                    rdata.append(field.cast_value(pk))
                else:
                    value = row[field.name]
                    rdata.append(field.cast_value(value))
            yield rdata

    def read(self, bucket):

        # Get rows
        rows = list(self.iter(bucket))

        return rows

    def write(self, bucket, rows):

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
