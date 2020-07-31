from types import FunctionType
import sys
import os

# findspark might not work for virtual env or non-standard
# install path but spark still may be available to python
try:
    import findspark
    findspark.init()
except:
    pass

import httpx
import pyspark
from pyspark.version import __version__ as PYSPARK_VERSION
import pyspark.sql.functions
import pyspark.ml

from pysparkrpc.proxy import Proxy, ProxyJavaObject
import pysparkrpc.api_client as api
from pysparkrpc.api_client import APIClient

__version__ = '0.4.0'

TARGET_OBJS = [
    pyspark.rdd.RDD,
    pyspark.rdd.PipelinedRDD,
    pyspark.context.SparkContext,
    pyspark.sql.context.SQLContext,
    pyspark.sql.DataFrame,
    pyspark.sql.DataFrameReader,
    pyspark.sql.DataFrameWriter,
    pyspark.sql.group.GroupedData,
    pyspark.sql.session.SparkSession,
    pyspark.sql.column.Column,
    pyspark.sql.udf.UDFRegistration
]

# pyspark will make classes available via the root module
# we need to specifically inject our clases into these as well
TARGET_ALIAS = {
    'SparkContext': 'pyspark',
    'SQLContext': 'pyspark.sql',
    'SparkSession': 'pyspark.sql'
}

TARGET_ALL_MODULES = [
    pyspark.ml.classification,
    pyspark.ml.clustering,
    pyspark.ml.regression,
    pyspark.ml.fpm,
    pyspark.ml.recommendation
]

MONKEY_PATCH_FUNCS = {
    pyspark.rdd.RDD: ['toDF']
}

MONKEY_PATCH_PROPS = {
    pyspark.context.SparkContext: ['_jsc']
}

for m in TARGET_ALL_MODULES:
    for c in getattr(m, '__all__'):
        TARGET_OBJS.append(getattr(m, c))

TARGET_FUNCTIONS = [
    pyspark.sql.functions
]

BUILTIN_FUNCS = [
    '__eq__', '__ne__', '__lt__', '__le__', '__ge__', '__gt__',
    '__neg__', '__add__', '__radd__', '__rsub__', '__mul__', '__rmul__',
    '__div__', '__rdiv__', '__rtruediv__', '__rmod__', '__pow__', '__rpow__',
    '__and__', '__or__', '__invert__', '__rand__', '__ror__',
    '__getattr__', '__getitem__', '__repr__'
]

PICKLE_FUNCS = [
    'parallelize',
    'createDataFrame',
    'predict'
]

def inject(url='http://localhost:8765', auth=None):
    if auth != None:
        APIClient.http = httpx.Client(timeout=60.0, headers={'Auth':auth})

    api.PROXY_URL=url

    _handle_spark_versions()

    for m in TARGET_FUNCTIONS:
        _build_functions(m)

    for c in TARGET_OBJS:
        obj_name = c.__name__
        proxy_class = type(obj_name, (Proxy,), {})
        globals()[obj_name] = proxy_class

        _build_class(c)

    for c in TARGET_OBJS:
        obj_name = c.__name__
        obj_module = c.__module__

        setattr(sys.modules[obj_module], obj_name, globals()[obj_name])

        if obj_name in TARGET_ALIAS:
            setattr(sys.modules[TARGET_ALIAS[obj_name]], obj_name, globals()[obj_name])

def cache(set_to=True):
    api.CACHING = set_to

    if set_to:
        print('PysparkRPC caching ON')
    else:
        print('PysparkRPC caching OFF')

def clear():
    APIClient.clear()

def _handle_spark_versions():
    major = PYSPARK_VERSION[0]

    if major == '3':
        import pyspark.ml.functions
        TARGET_FUNCTIONS.append(pyspark.ml.functions)

def _build_class(obj, path=None):
    obj_name = obj.__name__
    obj_module = obj.__module__

    # don't patch any non-pyspark objects
    if 'pyspark' not in obj_module:
        return

    if path == None:
        path = f'{obj.__module__}.{obj_name}'

    proxy_class = globals()[obj_name]
    setattr(proxy_class, '_path', path)

    obj_names = dir(obj)

    for f in BUILTIN_FUNCS:
        # don't override the builtin if pyspark didn't override it
        # built in functions return as the class wrapper_descriptor
        if f in obj_names and getattr(obj, f).__class__.__name__ != 'wrapper_descriptor':
            patch_code = compile(f'def {f}(self, *args, **kwargs): return APIClient.call(self._id, self._path, "{f}", args, kwargs)', '<string>', 'exec')
            patch_func = FunctionType(patch_code.co_consts[0], globals())
            setattr(proxy_class, f, patch_func)

    if obj in MONKEY_PATCH_FUNCS:
        for f in MONKEY_PATCH_FUNCS[obj]:
            patch_code = compile(f'def {f}(self, *args, **kwargs): return APIClient.call(self._id, self._path, "{f}", args, kwargs)', '<string>', 'exec')
            patch_func = FunctionType(patch_code.co_consts[0], globals())
            setattr(proxy_class, f, patch_func)

    if obj in MONKEY_PATCH_PROPS:
        for f in MONKEY_PATCH_PROPS[obj]:
            patch_code = compile(f'def {f}(self): return APIClient.call(self._id, self._path, "{f}", is_property=True)', '<string>', 'exec')
            patch_func = FunctionType(patch_code.co_consts[0], globals())
            setattr(proxy_class, f, property(patch_func))

    for f in obj_names:

        if ('__' in f and f not in BUILTIN_FUNCS) or f[0] == '_':
            continue

        class_name = getattr(obj, f).__class__.__name__

        if hasattr(getattr(obj, f), '__self__'):
            patch_code = compile(f'def {f}(cls, *args, **kwargs): return APIClient.call(None, cls._path, "{f}", args, kwargs)', '<string>', 'exec')
            patch_func = FunctionType(patch_code.co_consts[0], globals())

            setattr(proxy_class, f, classmethod(patch_func))
        elif class_name == 'property':
            patch_code = compile(f'def {f}(self, *args, **kwargs): return APIClient.call(self._id, self._path, "{f}", is_property=True)', '<string>', 'exec')
            patch_func = FunctionType(patch_code.co_consts[0], globals())

            setattr(proxy_class, f, property(patch_func))
        elif class_name == 'function':
            patch_code = compile(f'def {f}(self, *args, **kwargs): return APIClient.call(self._id, self._path, "{f}", args, kwargs)', '<string>', 'exec')
            patch_func = FunctionType(patch_code.co_consts[0], globals())

            setattr(proxy_class, f, patch_func)
        elif class_name != 'function' and class_name != 'type':
            prop_class = getattr(obj, f).__class__
            prop_class_name = prop_class.__name__

            c = type(prop_class_name, (Proxy,), {'_propclass':True})
            globals()[prop_class_name] = c

            _build_class(prop_class, path=f'{obj_module}.{obj_name}.{f}')

            # assuming the class has to be initialized
            # TODO: maybe this should be added to the base class init?
            setattr(proxy_class, f, c())

def _build_functions(module):
    for f in dir(module):
        class_name = getattr(module, f).__class__.__name__

        if '__' not in f and f[0] != '_' and class_name == 'function':
            patch_code = compile(f'def {f}(*args, **kwargs): return APIClient.call(None, "{module.__name__}", "{f}", args, kwargs)', '<string>', 'exec')
            patch_func = FunctionType(patch_code.co_consts[0], globals())
            setattr(module, f, patch_func)
