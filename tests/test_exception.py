import pytest

spark = pytest.spark

def test_exception():
    try:
        df = spark.read.json('/tmp/unknown/path.json')
    except Exception as e:
        assert 'Path does not exist' in str(e)
