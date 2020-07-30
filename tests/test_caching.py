import pytest

from pysparkrpc import cache
import pysparkrpc.api_client as api

spark = pytest.spark

def test_cached_result():
    df = spark.createDataFrame([{'foo':1,'bar':2},{'foo':1,'bar':2}], ['foo','bar'])
    result_df = df.filter('foo = 1')
    result_df2 = df.filter('foo = 1')

    assert result_df2._cached == True

def test_toggle_cache():
    cache(False)
    assert api.CACHING == False

    cache(True)
    assert api.CACHING == True

def test_toggle_cache_result():
    df = spark.createDataFrame([{'foo':1,'bar':2},{'foo':1,'bar':2}], ['foo','bar'])
    result_df = df.filter('foo = 1')

    cache(False)
    result_df2 = df.filter('foo = 1')

    assert result_df2._cached == False

    cache(True)

def test_no_cache_on_exception():
    query = 'select count(*) AS cnt from table'

    try:
        df = spark.sql(query)
    except:
        pass

    table_df = spark.createDataFrame([{'a':1,'b':2},{'a':1,'b':2}], ['a','b'])
    table_df.createOrReplaceTempView('table')

    rows = spark.sql(query).collect()

    assert rows[0].cnt == 2
