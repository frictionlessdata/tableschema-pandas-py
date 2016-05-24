# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest
from jsontableschema_pandas.utils import force_list


def test_force_list():
    assert force_list(1, int) == [1]
    assert force_list([1], int) == [1]


def test_force_list_error():
    with pytest.raises(TypeError) as excinfo:
        force_list('a', int)
    assert str(excinfo.value) == "'a' is not one of list, int"
