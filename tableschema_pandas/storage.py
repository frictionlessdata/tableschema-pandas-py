# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import six
import collections
import tableschema
import pandas as pd
from .mapper import Mapper


# Module API

class Storage(tableschema.Storage):

    # Public

    def __init__(self, dataframes=None):
        """https://github.com/frictionlessdata/tableschema-pandas-py#storage
        """

        # Set attributes
        self.__dataframes = dataframes or collections.OrderedDict()
        self.__descriptors = {}

        # Create mapper
        self.__mapper = Mapper()

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
        return list(sorted(self.__dataframes.keys()))

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
                    message = 'Bucket "%s" already exists' % bucket
                    raise tableschema.exceptions.StorageError(message)
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
                    message = 'Bucket "%s" doesn\'t exist' % bucket
                    raise tableschema.exceptions.StorageError(message)
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
                descriptor = self.__mapper.restore_descriptor(dataframe)

        return descriptor

    def iter(self, bucket):
        """https://github.com/frictionlessdata/tableschema-pandas-py#storage
        """

        # Check existense
        if bucket not in self.buckets:
            message = 'Bucket "%s" doesn\'t exist.' % bucket
            raise tableschema.exceptions.StorageError(message)

        # Prepare
        descriptor = self.describe(bucket)
        schema = tableschema.Schema(descriptor)

        # Yield rows
        for pk, row in self.__dataframes[bucket].iterrows():
            row = self.__mapper.restore_row(row, schema=schema, pk=pk)
            yield row

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
        new_data_frame = self.__mapper.convert_descriptor_and_rows(descriptor, rows)

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
