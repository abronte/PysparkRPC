import pytest

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
