import types
import functools
import base64
import pickle

import httpx
import cloudpickle

import pysparkapi

PROXY_URL = 'http://localhost:8765'

def _copy_func(f):
    g = types.FunctionType(f.__code__, f.__globals__, name=f.__name__,
                           argdefs=f.__defaults__,
                           closure=f.__closure__)
    g = functools.update_wrapper(g, f)
    return g

class APIClient(object):
    @classmethod
    def call(cls, object_id, path, function, args=(), kwargs={}, is_property=False, is_item=False, create=False):
        if function in pysparkapi.PICKLE_FUNCS:
            function_args = [
               str(base64.b64encode(cloudpickle.dumps(args)), 'utf-8'),
               str(base64.b64encode(cloudpickle.dumps(kwargs)), 'utf-8'),
            ]
        else:
            function_args = cls._prepare_args(args, kwargs)

        body = {
            'object_id': object_id,
            'path': path,
            'function': function,
            'args': function_args[0],
            'kwargs': function_args[1],
            'is_property': is_property,
            'is_item': is_item
        }

        print(body)

        r = httpx.post(PROXY_URL+'/call', json=body, timeout=60.0)
        resp = r.json()

        if resp['stdout'] != []:
            print('\n'.join(resp['stdout']))

        if resp['object']:
            if resp['object_id'] != None:
                obj_id = resp['object_id']

                # udfs
                if resp['class'] == 'function':
                    f_code = compile(f'def proxyfunc(*args, **kwargs): return APIClient.call("{obj_id}", None, None, args, kwargs)', '<string>', 'exec')
                    f_func = types.FunctionType(f_code.co_consts[0], globals())

                    return f_func
                else:
                    # don't initalize a new object if this is getting called from __init__
                    if create:
                        return obj_id
                    else:
                        return getattr(pysparkapi, resp['class'])(_id=obj_id)

            elif resp['class'] == 'pickle':
                return pickle.loads(base64.b64decode(resp['value']))
            else:
                return resp['value']
        else:
            return None

    @classmethod
    def _prepare_args(cls, args, kwargs):
        prepared_args = []
        prepared_kwargs = {}

        for a in args:
            arg_type = type(a)

            # pyspark objects can sometimes be in lists so we need to
            # check the list and send their id over so the server knows
            # what to retrieve
            if arg_type == list or arg_type == tuple:
                processed_list = []

                for x in a:
                    type_x = type(x)

                    if type_x == list or type_x == tuple:
                        processed_sub_list = []

                        for sub_x in x:
                            processed_sub_list.append(cls._proxy_obj_replace(sub_x))

                        processed_list.append(processed_sub_list)
                    else:
                        processed_list.append(cls._proxy_obj_replace(x))

                prepared_args.append(processed_list)
            elif arg_type == types.FunctionType:
                pickled_f = str(base64.b64encode(cloudpickle.dumps(_copy_func(a))), 'utf-8')
                prepared_args.append({'_CLOUDPICKLE': pickled_f})
            else:
                prepared_args.append(cls._proxy_obj_replace(a))

        for a in kwargs:
            v = kwargs[a]
            prepared_kwargs[a] = cls._proxy_obj_replace(v)

        # prepared_args = str(base64.b64encode(pickle.dumps(args, 2)), 'utf-8')
        # prepared_kwargs = str(base64.b64encode(pickle.dumps(kwargs, 2)), 'utf-8')

        return prepared_args, prepared_kwargs

    @classmethod
    def _proxy_obj_replace(cls, obj):
        if hasattr(obj, '_PROXY'):
            return {'_PROXY_ID': obj._id}
        else:
            return obj
