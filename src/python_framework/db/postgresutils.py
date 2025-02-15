import logging
from contextlib import contextmanager
from copy import deepcopy
from typing import List, Tuple

from sqlalchemy import create_engine
from sqlalchemy.engine import Connection
from sqlalchemy.orm.session import Session

from python_framework.config_utils import load_environment_variable
from python_framework.db.config import DBConfig
from python_framework.logger import ContextLogger, LogLevel

CONNECTION_POOL: List[Tuple["ConnectionDetails", Connection]] = []
LOGGER_KEY = "POSTGRES_UTILS"

LOGGER_INITIALIZED = False


def initialize_logger():
    global LOGGER_INITIALIZED

    if LOGGER_INITIALIZED:
        return

    enable_trace_logging = load_environment_variable(
        "ENABLE_PGUTILS_TRACE_LOGGING", "False"
    )

    if enable_trace_logging.lower() == "true":
        ContextLogger.instance().create_logger_for_context(LOGGER_KEY, LogLevel.TRACE)
    else:
        ContextLogger.instance().create_logger_for_context(LOGGER_KEY, LogLevel.INFO)

    LOGGER_INITIALIZED = True


class ConnectionDetails:
    def __init__(
        self,
        host,
        port,
        user,
        password,
        database="postgres",
        schema="public",
        dialect="postgresql",
        driver="pg8000",
        disable_ssl=False,
    ):
        self.dialect = dialect
        self.driver = driver
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.schema = schema
        self.disable_ssl = disable_ssl

    def get_connection_string(self):
        engine = (
            "{}".format(self.dialect)
            if self.driver is None
            else "{}+{}".format(self.dialect, self.driver)
        )
        return "{}://{}:{}@{}:{}/{}{}".format(
            engine,
            self.user,
            self.password,
            self.host,
            self.port,
            self.database,
            "?sslmode=disable" if self.disable_ssl else "",
        )

    def get_connection_args(self):
        args = {}

        return args

    def get_key(self):
        return f"{self.host}:{self.port}:{self.database}:{self.user}:{self.schema}"

    def __eq__(self, other):
        return (
            self.dialect == other.dialect
            and self.driver == other.driver
            and self.host == other.host
            and self.port == other.port
            and self.database == other.database
            and self.user == other.user
            and self.password == other.password
            and self.schema == other.schema
        )

    def __repr__(self):
        repr_dict = deepcopy(self.__dict__)
        repr_dict["password"] = "******"

        return str(repr_dict)

    @staticmethod
    def from_db_config(database_config: DBConfig) -> "ConnectionDetails":
        try:
            schema_used = (
                database_config.schema_id
                if database_config.use_raw_schema_id
                else getSchemaFromSchemaId(database_config.schema_id)
            )

            return ConnectionDetails(
                database_config.host,
                database_config.port,
                database_config.username,
                database_config.password,
                database=database_config.database_name,
                schema=schema_used,
                dialect="redshift" if database_config.use_redshift else "postgresql",
                disable_ssl=database_config.disable_ssl,
            )
        except:
            error_msg = (
                "Failed to create ConnectionDetails for database_config = [%s]\nPlease verify environment variables"
                % database_config
            )
            ContextLogger.error(LOGGER_KEY, error_msg)

            raise Exception(error_msg)


def create_db_engine(connection_details: ConnectionDetails, autocommit=False):
    # TODO: exception handling here
    engine = None

    if autocommit:
        engine = create_engine(
            connection_details.get_connection_string(),
            connect_args=connection_details.get_connection_args(),
            isolation_level="AUTOCOMMIT",
        )
    else:
        engine = create_engine(
            connection_details.get_connection_string(),
            connect_args=connection_details.get_connection_args(),
        )

    if connection_details.schema is not None and len(connection_details.schema) > 0:
        engine.execution_options(schema_translate_map={None: connection_details.schema})

    return engine


def create_connection(
    engine=None, connection_details: ConnectionDetails = None, autocommit=False
):
    ContextLogger.trace(
        LOGGER_KEY, "creating connection for [%s]" % repr(connection_details)
    )

    if engine is None:
        engine = create_db_engine(connection_details)

        if autocommit:
            engine.execution_options(isolation_level="AUTOCOMMIT")

    # TODO: exception handling here
    if autocommit:
        connection = engine.connect()
        connection.connection.connection.set_isolation_level(0)
        return connection
    else:
        return engine.connect()


def find_connection_in_pool(
    connection_details: ConnectionDetails, filter_in_transaction: bool = True
):
    ContextLogger.trace(
        LOGGER_KEY, "finding connection for [%s]" % repr(connection_details)
    )
    matched_in_transaction = 0

    for connection in CONNECTION_POOL:
        if connection is not None and connection[0] == connection_details:
            if connection[1].in_transaction() and filter_in_transaction:
                matched_in_transaction += 1
                continue
            else:
                ContextLogger.trace(LOGGER_KEY, "connection found")
                ContextLogger.trace(
                    LOGGER_KEY,
                    "connections skipped in transaction = [%d]"
                    % matched_in_transaction,
                )

                return connection[1]

    ContextLogger.trace(LOGGER_KEY, "no connection found")
    ContextLogger.trace(
        LOGGER_KEY, "connections skipped in transaction = [%d]" % matched_in_transaction
    )

    return None


def close_connection(connection, connection_details=None):
    global CONNECTION_POOL
    connection.close()

    if connection_details is None:
        return

    for i in range(0, len(CONNECTION_POOL)):
        if (
            CONNECTION_POOL[i] is not None
            and CONNECTION_POOL[i][0] == connection_details
        ):
            CONNECTION_POOL = CONNECTION_POOL[:i] + CONNECTION_POOL[i + 1 :]
            break


@contextmanager
def create_transaction(
    engine=None,
    connection=None,
    connection_details=None,
    keep_connection_alive=False,
    allow_nested_transaction=False,
):
    new_connection = connection is None

    if new_connection:
        connection = find_connection_in_pool(connection_details)

        if (
            connection is not None
            and not allow_nested_transaction
            and connection.in_transaction()
        ):
            connection = None
            keep_connection_alive = False

        if connection is None:
            connection = create_connection(engine, connection_details)

            if keep_connection_alive:
                CONNECTION_POOL.append((connection_details, connection))

    # TODO: exception handling here
    transaction = connection.begin()

    try:
        yield connection
    except Exception:
        logging.exception("rolling back transaction")
        transaction.rollback()
        raise
    else:
        transaction.commit()
    finally:
        if new_connection and connection is not None and not keep_connection_alive:
            close_connection(connection, connection_details)


@contextmanager
def create_non_transactional_connection(engine=None, connection_details=None):
    connection = create_connection(engine, connection_details, autocommit=True)

    try:
        yield connection
    except Exception:
        logging.exception("non-transactional connection failed")
        raise
    finally:
        close_connection(connection)


def getSchemaFromSchemaId(schema_id: str):
    return "_" + schema_id.replace("-", "")


@contextmanager
def create_session(connection_details=None, autocommit=False):
    engine = create_db_engine(connection_details)

    session = Session(engine)
    transaction = session.begin()

    try:
        yield session
    except Exception:
        logging.exception("rolling back transaction")
        transaction.rollback()
        raise
    else:
        transaction.commit()
    finally:
        pass
