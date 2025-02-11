from python_framework.config_utils import load_environment_variable
from python_framework.db.config import DBConfig
from python_framework.logger import ContextLogger, LogLevel


class ApplicationConfig:

    __instance: "ApplicationConfig" = None

    application_name: str
    api_host: str
    api_port: int

    database_config: DBConfig
    migrations_path: str

    def __init__(
        self,
        application_name: str,
        api_host: str,
        api_port: int,
        database_config: DBConfig,
        migrations_path: str,
    ) -> None:
        self.application_name = application_name
        self.api_host = api_host
        self.api_port = api_port
        self.database_config = database_config
        self.migrations_path = migrations_path

    @staticmethod
    def instance() -> "ApplicationConfig":
        if ApplicationConfig.__instance is None:
            ApplicationConfig.initialize()

        return ApplicationConfig.__instance

    @staticmethod
    def initialize() -> "ApplicationConfig":
        if ApplicationConfig.__instance is not None:
            return ApplicationConfig.__instance

        ContextLogger.sys_log(LogLevel.INFO, "[ApplicationConfig] initializing...")

        ApplicationConfig.__instance = ApplicationConfig(
            load_environment_variable("APPLICATION_NAME", error_on_none=True),
            load_environment_variable("API_HOST", error_on_none=True),
            int(load_environment_variable("API_PORT", error_on_none=True)),
            DBConfig(
                load_environment_variable("DATABASE_HOST", error_on_none=True),
                int(
                    load_environment_variable("DATABASE_PORT", 5432, error_on_none=True)
                ),
                load_environment_variable("DATABASE_NAME", error_on_none=True),
                load_environment_variable("DATABASE_USERNAME", error_on_none=True),
                load_environment_variable("DATABASE_PASSWORD", error_on_none=True),
                load_environment_variable("DATABASE_SCHEMA", error_on_none=True),
            ),
            load_environment_variable("DATABASE_MIGRATIONS_PATH", error_on_none=True),
        )

        ContextLogger.sys_log(LogLevel.INFO, "[ApplicationConfig] initialized.")
