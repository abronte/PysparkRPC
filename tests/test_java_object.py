import pytest

sc = pytest.sc

def test_java_object_hadoop_configuration():
   sc._jsc.hadoopConfiguration().set('foo', 'bar')
   val = sc._jsc.hadoopConfiguration().get('foo')

   assert val == 'bar'
