from sys import exc_info, stdout
import traceback
from config.application_config import ApplicationConfig
from controllers.app_watch import AppWatch
from controllers.example import ExampleController
from python_framework.graceful_killer import GracefulKiller
from python_framework.logger import ContextLogger


from python_framework.api_root.api_root import APIRoot
from python_framework.config_utils import load_environment_variable
from python_framework.db.connection_pool import ConnectionPool
from python_framework.db.connection_pool import (
    initialize_logger as connection_pool_initialize_logger,
)
from python_framework.db.dao.dao import initialize_logger as dao_initialize_logger
from python_framework.db.migrator import Migrator
from python_framework.db.postgresutils import ConnectionDetails
from python_framework.db.postgresutils import (
    initialize_logger as pgutils_initialize_logger,
)
from python_framework.graceful_killer import GracefulKiller
from python_framework.logger import ContextLogger, LogLevel


def init_configs():
    ApplicationConfig.initialize()


def init_database():
    dao_initialize_logger()
    connection_pool_initialize_logger()
    pgutils_initialize_logger()

    migrator = Migrator(
        ApplicationConfig.instance().migrations_path,
        ApplicationConfig.instance().database_config,
    )

    if not migrator.migrate():
        raise Exception("ERROR - [App] migrations failed")

    ConnectionPool.initialize(
        ConnectionDetails.from_db_config(ApplicationConfig.instance().database_config),
        max_pool_size=int(load_environment_variable("DATABASE_POOL_MAX_SIZE", 30)),
        initial_pool_size=int(
            load_environment_variable("DATABASE_POOL_INITIAL_SIZE", 5)
        ),
    )


def init():
    ContextLogger.initialize()

    # configs
    init_configs()

    # migrations
    init_database()

    GracefulKiller.initialize()
    AppWatch.initialize(ApplicationConfig.instance().application_name)

    # controllers
    ExampleController.initialize()

    # TODO: API stuff
    # APIRoot.initialize(
    #     ApplicationConfig.instance().application_name,
    #     ApplicationConfig.instance().api_host,
    #     ApplicationConfig.instance().api_port,
    #     API_RESOURCES,
    # )


def run():
    try:
        # Should be last, just before APIRoot
        AppWatch.instance().start()

        APIRoot.run()
    except:
        ContextLogger.sys_log(
            LogLevel.ERROR, "error caught during app execution: %s" % repr(exc_info())
        )
        traceback.print_exc(file=stdout)
    finally:
        GracefulKiller.instance().exit_gracefully()


if __name__ == "__main__":
    init()
    run()
