from abc import ABC
from typing import Dict, List, Type

from sqlalchemy import text
from sqlalchemy.engine.base import Connection

from python_framework.config_utils import load_environment_variable
from python_framework.db.config import DBConfig
from python_framework.db.dao.objects import DAOQuery, DAORecord
from python_framework.db.transaction_manager import TransactionManager
from python_framework.logger import ContextLogger, LogLevel

# TODO: define returns of execute queries to be generic:
# https://geekyisawesome.blogspot.com/2020/10/python-generic-methodsfunctions.html
# T = TypeVar('T')
# return_type: Type[T] = DAORecord

SELECT_QUERY_KEY = "SELECT"
SELECT_ALL_QUERY_KEY = "SELECT_ALL"
INSERT_QUERY_KEY = "INSERT"
UPDATE_QUERY_KEY = "UPDATE"
UPSERT_QUERY_KEY = "UPSERT"
DELETE_QUERY_KEY = "DELETE"

LOGGER_KEY = "DAO"

LOGGER_INITIALIZED = False


def initialize_logger():
    global LOGGER_INITIALIZED

    if LOGGER_INITIALIZED:
        return

    enable_trace_logging = load_environment_variable(
        "ENABLE_DAO_TRACE_LOGGING", "False"
    )

    if enable_trace_logging.lower() == "true":
        ContextLogger.instance().create_logger_for_context(LOGGER_KEY, LogLevel.TRACE)
    else:
        ContextLogger.instance().create_logger_for_context(LOGGER_KEY, LogLevel.INFO)

    LOGGER_INITIALIZED = True


class DAO(ABC):

    queries: Dict[str, Type[DAOQuery]]

    @classmethod
    def to_query(cls, query_key: str, query_kwargs: dict = None) -> DAOQuery:
        if query_key not in cls.queries:
            ContextLogger.error(
                LOGGER_KEY, "no query registered with key = [%s]" % query_key
            )

            return None

        if query_kwargs is not None:
            return cls.queries[query_key](**query_kwargs)
        else:
            return cls.queries[query_key]()

    @classmethod
    def execute_query(
        cls,
        query_key: str,
        database_config: DBConfig = None,
        connection: Connection = None,
        return_count_only=False,
        query_kwargs: dict = None,
    ) -> List[Type[DAORecord]]:
        query = cls.to_query(query_key, query_kwargs)

        if query is None:
            return None

        return DAO._execute_query(
            query,
            database_config=database_config,
            connection=connection,
            return_count_only=return_count_only,
        )

    # Default queries

    @classmethod
    def to_select_query(cls, **query_kwargs) -> Type[DAOQuery]:
        return cls.to_query(SELECT_QUERY_KEY, query_kwargs)

    @classmethod
    def execute_select(
        cls,
        database_config: DBConfig = None,
        connection: Connection = None,
        **query_kwargs,
    ) -> List[Type[DAORecord]]:
        return cls.execute_query(
            SELECT_QUERY_KEY,
            database_config=database_config,
            connection=connection,
            query_kwargs=query_kwargs,
        )

    @classmethod
    def to_select_all_query(cls, **query_kwargs) -> Type[DAOQuery]:
        return cls.to_query(SELECT_ALL_QUERY_KEY, query_kwargs)

    @classmethod
    def execute_select_all(
        cls,
        database_config: DBConfig = None,
        connection: Connection = None,
        **query_kwargs,
    ) -> List[Type[DAORecord]]:
        return cls.execute_query(
            SELECT_ALL_QUERY_KEY,
            database_config=database_config,
            connection=connection,
            query_kwargs=query_kwargs,
        )

    @classmethod
    def to_insert_query(cls, **query_kwargs) -> Type[DAOQuery]:
        return cls.to_query(INSERT_QUERY_KEY, query_kwargs)

    @classmethod
    def execute_insert(
        cls,
        database_config: DBConfig = None,
        connection: Connection = None,
        **query_kwargs,
    ) -> List[Type[DAORecord]]:
        return cls.execute_query(
            INSERT_QUERY_KEY,
            database_config=database_config,
            connection=connection,
            query_kwargs=query_kwargs,
        )

    @classmethod
    def to_update_query(cls, **query_kwargs) -> Type[DAOQuery]:
        return cls.to_query(UPDATE_QUERY_KEY, query_kwargs)

    @classmethod
    def execute_update(
        cls,
        database_config: DBConfig = None,
        connection: Connection = None,
        **query_kwargs,
    ) -> List[Type[DAORecord]]:
        return cls.execute_query(
            UPDATE_QUERY_KEY,
            database_config=database_config,
            connection=connection,
            query_kwargs=query_kwargs,
        )

    @classmethod
    def to_upsert_query(cls, **query_kwargs) -> Type[DAOQuery]:
        return cls.to_query(UPSERT_QUERY_KEY, query_kwargs)

    @classmethod
    def execute_upsert(
        cls,
        database_config: DBConfig = None,
        connection: Connection = None,
        **query_kwargs,
    ) -> List[Type[DAORecord]]:
        return cls.execute_query(
            UPSERT_QUERY_KEY,
            database_config=database_config,
            connection=connection,
            query_kwargs=query_kwargs,
        )

    @classmethod
    def to_delete_query(cls, **query_kwargs) -> Type[DAOQuery]:
        return cls.to_query(DELETE_QUERY_KEY, query_kwargs)

    @classmethod
    def execute_delete(
        cls,
        database_config: DBConfig = None,
        connection: Connection = None,
        **query_kwargs,
    ) -> List[Type[DAORecord]]:
        return cls.execute_query(
            DELETE_QUERY_KEY,
            database_config=database_config,
            connection=connection,
            query_kwargs=query_kwargs,
        )

    @staticmethod
    def _execute_statement(
        query: Type[DAOQuery],
        sql: str,
        field_map: Dict,
        return_count_only,
        connection: Connection,
    ) -> List[Type[DAORecord]]:
        ContextLogger.trace(
            LOGGER_KEY,
            "executing query - sql = [%s], field_map = [%s]" % (sql, field_map),
        )

        results = connection.execute(text(sql), field_map)

        ContextLogger.trace(
            LOGGER_KEY,
            "results = [%s], rowcount = [%d]"
            % (repr(results), -1 if results is None else results.rowcount),
        )

        if return_count_only:
            return results.rowcount

        if results is None or results.rowcount == 0:
            return []

        mapped_results = []

        for result in results:
            mapped_results.append(query.map_result(result))

        return mapped_results

    @staticmethod
    def _execute_query(
        query: Type[DAOQuery],
        database_config: DBConfig = None,
        connection: Connection = None,
        return_count_only=False,
    ) -> List[Type[DAORecord]]:
        sql, field_map = query.to_sql()

        if connection is not None:
            return DAO._execute_statement(
                query, sql, field_map, return_count_only, connection
            )

        with TransactionManager(database_config=database_config) as connection:
            return DAO._execute_statement(
                query, sql, field_map, return_count_only, connection
            )
