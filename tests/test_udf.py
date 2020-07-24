import pytest

import pyspark.sql.functions as F

spark = pytest.spark

def test_udf_function():
    def squared(s):
          return s * s

    squared_udf = F.udf(squared)

    df = spark.createDataFrame([{'id':2},{'id': 2}], ['id'])
    rows = df.select(squared_udf('id').alias('id')).collect()

    assert rows[0].id == '4'

def test_udf_sql_function():
    def add(s):
          return s + 1

    spark.udf.register("add", add)

    df = spark.createDataFrame([{'id':2},{'id': 2}], ['id'])
    df.createOrReplaceTempView('table')

    rows = spark.sql('select add(id) AS id FROM table').collect()

    assert rows[0].id == '3'
