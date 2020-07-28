from multiprocessing import Process

import pytest

import pysparkrpc
pysparkrpc.inject()

from pyspark.sql.session import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext

def pytest_sessionstart(session):
    print('Starting spark context')

    pysparkrpc.APIClient.clear()

    sc = SparkContext()
    sqlContext = SQLContext(sc)
    spark = SparkSession.builder.getOrCreate()

    pytest.spark = spark
    pytest.sc = sc
    pytest.sqlcontext = sqlContext
