from pysparkapi.api_client import APIClient

import cloudpickle
import sys

class Proxy(object):
    _PROXY = True
    _path = None
    _propclass = False

    def __init__(self, *args, **kwargs):
        self._kwargs = kwargs
        self._class = self.__class__.__name__
        self._args = args

        print(f'object init {self._class}')
        print(self._path)
        print(args)
        print(kwargs)
        print(f'_propclass: {self._propclass}')

        self._id = None

        if '_id' in kwargs:
            self._id = kwargs['_id']
        elif self._path and self._propclass == False:
            self._id = APIClient.call(None, self._path, None, args, kwargs, create=True)
