import pytest

import pyspark.sql.functions as F
from pyspark.sql.types import Row, StructType, StructField, IntegerType

spark = pytest.spark

def test_create_dataframe():
    rows = [
        Row(foo=1, bar=2, baz=3),
        Row(foo=1, bar=2, baz=3),
        Row(foo=1, bar=2, baz=3)
    ]

    schema = StructType([
        StructField('foo', IntegerType(), True),
        StructField('bar', IntegerType(), True),
        StructField('baz', IntegerType(), True),
    ])

    df = spark.createDataFrame(rows, schema)

    assert df.count() == 3

def test_dataframe_collect():
    df = spark.createDataFrame([{'id':1},{'id': 2}])

    assert len(df.collect()) == 2

def test_dataframe_show():
    df = spark.createDataFrame([{'id':1},{'id': 2}])
    df.show()

def test_functions():
    df = spark.createDataFrame([{'col':'foo'}], ['col'])
    rows = df.select(F.sha2(df.col, 256).alias('hashed')).collect()

    assert rows[0].hashed == '2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae'

def test_to_pandas():
    df = spark.createDataFrame([{'id':1},{'id': 2}])
    df_pd = df.toPandas()

    assert df_pd.__module__ == 'pandas.core.frame'

def test_dataframe_filter_conditions():
    df = spark.createDataFrame([{'id':1},{'id': 2},{'id': 3}], ['id'])

    assert df.filter(df.id == 1).count() == 1
    assert df.filter(1 == df.id).count() == 1
    assert df.filter(df.id != 1).count() == 2
    assert df.filter(1 != df.id).count() == 2
    assert df.filter((df.id == 1) | (df['id'] == 2)).count() == 2
    assert df.filter((df.id == 1) & (df['id'] != 3)).count() == 1

def test_groupby():
    df = spark.createDataFrame([{'id':1},{'id': 1},{'id': 3}], ['id'])

    counted = df.groupBy('id').count()

    assert 'count' in counted.columns

    agg_df = df.groupBy('id').agg(F.count('id').alias('count_alias'))

    assert 'count_alias' in agg_df.columns
