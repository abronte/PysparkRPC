import uuid
import sys
import types
import pickle
import base64
import threading
import queue
import logging

import pysparkrpc
from pysparkrpc.server.capture import Capture
from pysparkrpc.server.logger import logger, configure_logging

logger = logging.getLogger()

import pyspark
import pyspark.sql.functions
import cloudpickle

from flask import Flask, request, jsonify
from flask.logging import default_handler
app = Flask(__name__)

OBJECTS = {}
RESPONSE_CACHE = {}
IN_PROGRESS = None

REQ_QUEUE = queue.Queue()
RESP_QUEUE = queue.Queue()

def retrieve_object(obj):
    global OBJECTS

    if type(obj) == dict and '_PROXY_ID' in obj:
        id = obj['_PROXY_ID']
        logger.info('Retrieving object id: %s' % id)

        return OBJECTS[id]
    else:
        return obj

def parse_request_args(function, request_args, request_kwargs):
    global OBJECTS

    args = []
    kwargs = {}

    if function in pysparkrpc.PICKLE_FUNCS:
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

        class_str = str(obj.__class__)

        # if the resulting object can be pickled just send the raw object
        # REFACTOR:
        if 'types.Row' in obj_str or 'pandas.' in obj_str or list == type(obj):
            logger.info('Pickling object type %s' % (str(type(obj))))
            result['class'] = 'pickle'
            result['value'] = str(base64.b64encode(pickle.dumps(obj, 2)), 'utf-8')
        elif 'pyspark' in class_str or 'py4j' in class_str or type(obj) == types.FunctionType:
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

def call_worker():
    global OBJECTS, REQ_QUEUE, RESP_QUEUE, RESPONSE_CACHE, IN_PROGRESS, Capture, logger

    while True:
        req = REQ_QUEUE.get()

        logger.info('Request:')
        logger.info(req)

        digest = req['digest']
        IN_PROGRESS = digest

        if digest in RESPONSE_CACHE:
            logger.info(f'Returning cached response for {digest}')

            resp = RESPONSE_CACHE[digest]
            resp['cached'] = True

            RESP_QUEUE.put(resp)
            continue

        res_obj = None

        resp = {
            'status': 'complete',
            'object': False,
            'object_id': None,
            'module': None,
            'class': None,
            'exception': None,
            'stdout': [],
            'cached': False
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

        try:
            with Capture() as stdout:
                if req['is_property']:
                    res_obj = callable_obj
                elif req['is_item']:
                    res_obj = callable_obj[req['function']]
                else:
                    res_obj = callable_obj(*args, **kwargs)

                resp['stdout'] = stdout
        except Exception as e:
            resp['exception'] = str(e)

        resp = handle_object(res_obj, resp)

        logger.info('Response:')
        logger.info(resp)

        if req['cache'] and resp['exception'] == None:
            RESPONSE_CACHE[digest] = resp

        IN_PROGRESS = None
        RESP_QUEUE.put(resp)
        REQ_QUEUE.task_done()

@app.before_request
def authenticate():
    if app.auth_token != None:
        auth = None

        if 'Auth' in request.headers:
            auth = request.headers['Auth']

        if auth != app.auth_token:
            return 'Authentication failed', 401


@app.route('/call', methods=['POST'])
def call():
    req = request.json

    if IN_PROGRESS != req['digest']:
        REQ_QUEUE.put(req)

    return 'OK'

@app.route('/response', methods=['GET'])
def response():
    if RESP_QUEUE.empty():
        resp = {'status':'pending'}
    else:
        resp = RESP_QUEUE.get()
        RESP_QUEUE.task_done()

    return jsonify(resp)

@app.route('/health', methods=['GET'])
def health():
    return 'OK'

@app.route('/clear', methods=['POST', 'GET'])
def clear():
    global OBJECTS, RESPONSE_CACHE

    if len(OBJECTS) > 0:
        sc = pyspark.SparkContext.getOrCreate()
        sc.stop()

        OBJECTS = {}
        RESPONSE_CACHE = {}

    return 'OK'

def run(*args, **kwargs):
    threading.Thread(target=call_worker, daemon=True).start()

    if 'debug' not in kwargs or ('debug' in kwargs and kwargs['debug'] == False):
        app.logger.removeHandler(default_handler)
        app.logger = logger

        logger.info('Starting pysparkrpc web server')

    if 'auth' in kwargs and kwargs['auth'] != '':
        app.auth_token = kwargs['auth']
    else:
        app.auth_token = None

    del kwargs['auth']

    if 'port' not in kwargs:
        kwargs['port'] = 8765

    app.run(*args, **kwargs)

if __name__ == '__main__':
	configure_logging(True, 'INFO')
	run(host='127.0.0.1', debug=True, use_reloader=False, port=8765)
