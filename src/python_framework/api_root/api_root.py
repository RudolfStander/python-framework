from types import ModuleType

from flask import Flask
from flask_cors import CORS
from werkzeug import serving

from dynamic_loader import load_submodules

parent_log_request = serving.WSGIRequestHandler.log_request


class APIRoot(object):

    __instance: "APIRoot" = None

    application_name: str
    host: str
    port: int
    app: Flask = None

    # TODO: keep registered routes in map ?

    # Initialization #

    def __init__(self, application_name: str, host: str, port: int):
        self.app = None
        self.application_name = application_name
        self.host = host
        self.port = port

    @staticmethod
    def instance():
        return APIRoot.__instance

    @staticmethod
    def initialize(application_name: str, host: str, port: int, resources_module: ModuleType):
        if APIRoot.__instance is not None:
            return APIRoot.__instance

        print("INFO - [APIRoot] initializing...")

        APIRoot.__instance = APIRoot(application_name, host, port)
        APIRoot.__instance.app = Flask(application_name, static_url_path="")
        CORS(APIRoot.__instance.app, resources={r"/*": {"origins": "*"}})

        filter_healthcheck_logs()

        APIRoot.__instance.register_routes(resources_module)

        APIRoot.__instance.print_routes()

        return APIRoot.__instance

    def register_routes(self, resources_module: ModuleType):
        modules = load_submodules(resources_module, ["register"])

        for module in modules:
            module.register(self)

    @staticmethod
    def run():
        APIRoot.__instance.app.run(host=APIRoot.__instance.host, port=APIRoot.__instance.port)

    @staticmethod
    def stop():
        APIRoot.__instance.app
        # TODO
        pass

    def register_route(self, blueprint):
        self.app.register_blueprint(blueprint)

    def print_routes(self):
        routes = []
        longest_path = 0

        for rule in self.app.url_map.iter_rules():
            path = str(rule.rule)
            methods = rule.methods
            endpoint = str(rule.endpoint)

            method = None

            if "GET" in methods:
                method = "GET"
            elif "POST" in methods:
                method = "POST"
            elif "DELETE" in methods:
                method = "DELETE"
            elif "PUT" in methods:
                method = "PUT"

            routes.append((method, path, endpoint))

            if len(path) > longest_path:
                longest_path = len(path)

        sorted_routes = sorted(routes, key=lambda route: route[1])

        print("\n--- API ROUTES ---\n")

        format_string = "%%6s  %%-%ds  ->  %%s" % longest_path

        for route in sorted_routes:
            print(format_string % route)

        print("\n------------------\n")


def log_request(self, *args, **kwargs):
    if self.path == "/api/healthcheck":
        return

    parent_log_request(self, *args, **kwargs)


def filter_healthcheck_logs():
    serving.WSGIRequestHandler.log_request = log_request
