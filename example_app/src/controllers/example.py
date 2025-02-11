from sys import exc_info, stdout
import traceback
from typing import List, Union
from db.daos.example import ExampleRecord, ExampleDAO
from objects.example import ExampleObject
from python_framework.logger import ContextLogger
from config.application_config import ApplicationConfig


class ExampleController:

    _logger_key: str = None

    _instance: "ExampleController" = None

    def __init__(self):
        self._logger_key = "ExampleController"

    @staticmethod
    def initialize() -> "ExampleController":
        if ExampleController._instance is not None:
            return ExampleController._instance

        ExampleController._instance = ExampleController()

        return ExampleController._instance

    def load(self) -> List[ExampleObject]:
        pass

    def persist(self, object: ExampleObject) -> Union[ExampleObject, None]:
        try:
            results: List[ExampleRecord] = None

            if object.last_updated is None:
                ContextLogger.debug(
                    self._logger_key, "Inserting NEW object with id [%s]" % object.id
                )
                results: List[ExampleRecord] = ExampleDAO.execute_insert(
                    ApplicationConfig.instance().database_config,
                    **object.to_record().generate_insert_query_args(),
                )
            else:
                ContextLogger.debug(
                    self._logger_key,
                    "Updating EXISTING object with id [%s]" % object.id,
                )
                results: List[ExampleRecord] = ExampleDAO.execute_update(
                    ApplicationConfig.instance().database_config,
                    **object.to_record().generate_update_query_args(),
                )

            if results is None or len(results) == 0:
                error_str = (
                    "Failed to persist object with id = [%s], error = [Insert/update returned zero records]"
                    % (object.id)
                )
                ContextLogger.error(self._logger_key, error_str)

                raise Exception(error_str)

            return ExampleObject.init_from_record(results[0])
        except:
            error_str = "Failed to persist Controller with id = [%s], error = [%s]" % (
                object.id,
                repr(exc_info()),
            )
            ContextLogger.error(self._logger_key, error_str)
            traceback.print_exc(file=stdout)

            raise Exception(error_str)

        return None
