from multiprocessing import Process

import pytest

import pysparkrpc
pysparkrpc.inject(url='http://127.0.0.1:8765', auth='abc123')

from pyspark.sql.session import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext

def pytest_sessionstart(session):
    print('Starting spark context')

    pysparkrpc.clear()

    sc = SparkContext()
    sqlContext = SQLContext(sc)
    spark = SparkSession.builder.getOrCreate()

    pytest.spark = spark
    pytest.sc = sc
    pytest.sqlcontext = sqlContext
