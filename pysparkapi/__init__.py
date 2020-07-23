from types import FunctionType
import sys

import pyspark
import pyspark.sql.functions
import pyspark.ml
import pyspark.ml.functions

from pysparkapi.proxy import Proxy
from pysparkapi.api_client import APIClient

import cloudpickle
import sys

__version__ = '0.1.0'

TARGET_OBJS = [
    pyspark.rdd.RDD,
    pyspark.rdd.PipelinedRDD,
    pyspark.context.SparkContext,
    pyspark.sql.context.SQLContext,
    pyspark.sql.DataFrame,
    pyspark.sql.DataFrameReader,
    pyspark.sql.DataFrameWriter,
    pyspark.sql.session.SparkSession,
    pyspark.sql.column.Column,
    pyspark.sql.udf.UDFRegistration,
]

TARGET_ALL_MODULES = [
    pyspark.ml.classification,
    pyspark.ml.clustering,
    pyspark.ml.regression
]

for m in TARGET_ALL_MODULES:
    for c in getattr(m, '__all__'):
        TARGET_OBJS.append(getattr(m, c))

TARGET_FUNCTIONS = [
    pyspark.sql.functions,
    pyspark.ml.functions
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
    'createDataFrame'
]

def inject():
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

def _build_class(obj, path=None):
    obj_name = obj.__name__
    obj_module = obj.__module__

    if 'pyspark' not in obj_module:
        print('NOT A PYSPARK OBJECT, SKIPPING')
        return

    if path == None:
        path = f'{obj.__module__}.{obj_name}'
    else:
        print(f'Forcing path: {path}')

    print(f'Processing {path}')

    proxy_class = globals()[obj_name]
    setattr(proxy_class, '_path', path)

    obj_names = dir(obj)

    for f in BUILTIN_FUNCS:
        if f in obj_names:
            patch_code = compile(f'def {f}(self, *args, **kwargs): return APIClient.call(self._id, self._path, "{f}", args, kwargs)', '<string>', 'exec')
            patch_func = FunctionType(patch_code.co_consts[0], globals())
            setattr(proxy_class, f, patch_func)
            print(f'{f} built in')

    for f in obj_names:
        print(f)

        if ('__' in f and f not in BUILTIN_FUNCS) or f[0] == '_':
            continue

        class_name = getattr(obj, f).__class__.__name__

        # clean this up, just call API client directly
        if hasattr(getattr(obj, f), '__self__'):
            print(f'{f} is a class method')

            patch_code = compile(f'def {f}(cls, *args, **kwargs): return APIClient.call(None, cls._path, "{f}", args, kwargs)', '<string>', 'exec')
            patch_func = FunctionType(patch_code.co_consts[0], globals())

            setattr(proxy_class, f, classmethod(patch_func))
        # clean this up, just call APIClietn directly
        elif class_name == 'property':
            patch_code = compile(f'def {f}(self, *args, **kwargs): return APIClient.call(self._id, self._path, "{f}", is_property=True)', '<string>', 'exec')
            patch_func = FunctionType(patch_code.co_consts[0], globals())
            setattr(proxy_class, f, property(patch_func))
            print(f'{f} property')
        elif class_name == 'function':
            patch_code = compile(f'def {f}(self, *args, **kwargs): return APIClient.call(self._id, self._path, "{f}", args, kwargs)', '<string>', 'exec')
            patch_func = FunctionType(patch_code.co_consts[0], globals())
            setattr(proxy_class, f, patch_func)
            print(f'{f} function')
        elif class_name != 'function' and class_name != 'type':
            print(f'{f} is a class stored in a property as class {class_name}')

            # WORKING: creating subclass
            prop_class = getattr(obj, f).__class__
            prop_class_name = prop_class.__name__

            print(prop_class)
            print(prop_class_name)

            c = type(prop_class_name, (Proxy,), {'_propclass':True})
            globals()[prop_class_name] = c

            _build_class(prop_class, path=f'{obj_module}.{obj_name}.{f}')

            print('')

            # assuming the class has to be initialized
            # TODO: maybe this should be added to the base class init?
            setattr(proxy_class, f, c())

def _build_functions(module):
    for f in dir(module):
        class_name = getattr(module, f).__class__.__name__

        if '__' not in f and f[0] != '_' and class_name == 'function':
            print(f)

            patch_code = compile(f'def {f}(*args, **kwargs): return APIClient.call(None, "{module.__name__}", "{f}", args, kwargs)', '<string>', 'exec')
            patch_func = FunctionType(patch_code.co_consts[0], globals())
            setattr(module, f, patch_func)
