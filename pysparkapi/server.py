import uuid
from io import StringIO
import sys
import types
import pickle
import base64

import pysparkapi
import pyspark
import pyspark.sql.functions
import cloudpickle

from flask import Flask, request, jsonify
app = Flask(__name__)

OBJECTS = {}

class Capture(list):
    def __enter__(self):
        self._stdout = sys.stdout
        sys.stdout = self._stringio = StringIO()
        return self

    def __exit__(self, *args):
        self.extend(self._stringio.getvalue().splitlines())
        del self._stringio
        sys.stdout = self._stdout

def retrieve_object(obj):
    global OBJECTS

    if type(obj) == dict and '_PROXY_ID' in obj:
        id = obj['_PROXY_ID']
        print('Retrieving object id: %s' % id)

        return OBJECTS[id]
    else:
        return obj

def parse_request_args(function, request_args, request_kwargs):
    global OBJECTS

    args = []
    kwargs = {}

    if function in pysparkapi.PICKLE_FUNCS:
        args = cloudpickle.loads(base64.b64decode(request_args))
        kwargs = cloudpickle.loads(base64.b64decode(request_kwargs))
    else:
        for a in request_args:
            arg_type = type(a)

            if arg_type == dict:
                if '_CLOUDPICKLE' in a:
                    obj = cloudpickle.loads(base64.b64decode(a['_CLOUDPICKLE']))
                    args.append(obj)
                else:
                    args.append(retrieve_object(a))
            # pyspark objects can sometimes be in lists so we need to
            # check the list and lookup any objects
            elif arg_type == list or arg_type == tuple:
                processed_list = []

                for x in a:
                    type_x = type(x)

                    if type_x == list or type_x == tuple:
                        processed_sub_list = []

                        for sub_x in x:
                            processed_sub_list.append(retrieve_object(sub_x))

                        processed_list.append(processed_sub_list)
                    else:
                        processed_list.append(retrieve_object(x))

                args.append(processed_list)
            # spark strictly typechecks some arguments expecting strings but
            # decoding json will turn strings into unicode objects
            elif arg_type == str:
                args.append(str(a))
            else:
                args.append(a)

        for k in request_kwargs:
            v = request_kwargs[k]

            kwargs[k] = retrieve_object(v)

    return args, kwargs

# checks if any objects are created via a function call
# and returns the corresponding object
#
# pandas: pickles and returns a pandas dataframe
# pyspark.*: returns class and corresponding object id
def handle_object(obj, result={}):
    global OBJECTS

    obj_str = str(type(obj))

    if obj is not None:
        result['object'] = True
        result['class'] = obj.__class__.__name__

        # if the resulting object can be pickled just send the raw object
        # REFACTOR:
        if 'types.Row' in obj_str or 'pandas.' in obj_str or list == type(obj):
            print('Pickling object type %s' % (str(type(obj))))
            result['class'] = 'pickle'
            result['value'] = str(base64.b64encode(pickle.dumps(obj, 2)), 'utf-8')
        elif 'pyspark' in str(obj.__class__) or type(obj) == types.FunctionType:
            id = str(uuid.uuid4())
            OBJECTS[id] = obj

            result['object'] = True
            result['object_id'] = id
            result['path'] = f'{obj.__module__}.{obj.__class__.__name__}'
            result['module'] = obj.__module__
            result['class'] = obj.__class__.__name__
        else:
            result['value'] = obj

    return result

@app.route('/call', methods=['POST'])
def call():
    global OBJECTS

    print('/call')

    req = request.json

    print('Request:')
    print(req)
    res_obj = None

    resp = {
        'object': False,
        'object_id': None,
        'module': None,
        'class': None,
        'exception': False,
        'stdout': None
    }

    if req['object_id'] != None:
        obj = OBJECTS[req['object_id']]

        if req['function'] != None:
            callable_obj = getattr(obj, req['function'])
        else:
            callable_obj = obj
    else:
        # have to loop through the module path and getattr
        # one by one because importing the whole module str
        # isn't always valid. For example pyspark.sql.session.SparkSession.Builder
        module_paths = req['path'].split('.')
        base_module = __import__(module_paths[0])

        module = base_module

        if len(module_paths) > 1:
            for m in module_paths[1:]:
                module = getattr(module, m)

        if req['function'] != None:
            callable_obj = getattr(module, req['function'])
        # if no function is passed, it means the base object needs init
        else:
            callable_obj = module

    args, kwargs = parse_request_args(req['function'], req['args'], req['kwargs'])
    print(args)
    print(kwargs)

    with Capture() as stdout:
        if req['is_property']:
            res_obj = callable_obj
        elif req['is_item']:
            res_obj = callable_obj[req['function']]
        else:
            res_obj = callable_obj(*args, **kwargs)

    resp['stdout'] = stdout

    print(res_obj)

    resp = handle_object(res_obj, resp)

    print('Response:')
    print(resp)
    return jsonify(resp)

@app.route('/health', methods=['GET'])
def health():
    return 'OK'

@app.route('/clear', methods=['POST', 'GET'])
def clear():
    global OBJECTS

    if len(OBJECTS) > 0:
        sc = pyspark.SparkContext.getOrCreate()
        sc.stop()

        OBJECTS = {}

    return 'OK'

def run(*args, **kwargs):
    if 'port' not in kwargs:
        kwargs['port'] = 8765

    app.run(*args, **kwargs)

if __name__ == '__main__':
    app.run(host='127.0.0.1', debug=True, use_reloader=False, port=8765)
    # app.run(debug=True, use_reloader=False, port=8765, certfile='/etc/ssl/localhost/localhost.crt', keyfile='/etc/ssl/localhost/localhost.key')
