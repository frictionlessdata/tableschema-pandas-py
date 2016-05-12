import datetime
import pandas as pd

from jsontableschema_pandas.mappers import _convert_dtype


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
