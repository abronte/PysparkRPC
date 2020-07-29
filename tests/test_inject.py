import pytest

from pysparkrpc.proxy import Proxy

def test_spark_context_proxy_object():
    from pyspark import SparkContext as sc1
    from pyspark.context import SparkContext as sc2

    assert sc1.__bases__[0] == Proxy
    assert sc2.__bases__[0] == Proxy

def test_sql_context_proxy_object():
    from pyspark.sql import SQLContext as sqlctx1
    from pyspark.sql.context import SQLContext as sqlctx2

    assert sqlctx1.__bases__[0] == Proxy
    assert sqlctx2.__bases__[0] == Proxy

def test_spark_session_proxy_object():
    from pyspark.sql import SparkSession as s1
    from pyspark.sql.session import SparkSession as s2

    assert s1.__bases__[0] == Proxy
    assert s2.__bases__[0] == Proxy
