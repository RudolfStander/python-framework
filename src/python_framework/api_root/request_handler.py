import functools
from sys import exc_info

from flask import request as flask_request


class APIContext(object):
    request: flask_request
    request_arguments: dict

    def __init__(self, request: flask_request, request_arguments={}):
        self.request = request
        self.request_arguments = request_arguments

    def __repr__(self):
        return str(self.__dict__)


def handle_request(request, request_function, request_arguments={}):
    api_context = APIContext(request, request_arguments)

    return request_function(api_context)


def request_handler():
    def decorator_request_handler(func):
        @functools.wraps(func)
        def wrapper_request_handler(*args, **kwargs):
            return handle_request(flask_request, func, kwargs)

        return wrapper_request_handler

    return decorator_request_handler
