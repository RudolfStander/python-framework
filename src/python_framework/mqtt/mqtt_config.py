from config_utils import load_environment_variable
from logger import ContextLogger, LogLevel


class MQTTConfig:

    __instance: "MQTTConfig" = None

    client_id: str
    mqtt_host: str
    mqtt_port: int

    def __init__(self, client_id: str, mqtt_host: str, mqtt_port: int) -> None:
        self.client_id = client_id
        self.mqtt_host = mqtt_host
        self.mqtt_port = mqtt_port

    @staticmethod
    def instance() -> "MQTTConfig":
        if MQTTConfig.__instance is None:
            MQTTConfig.initialize()

        return MQTTConfig.__instance

    @staticmethod
    def initialize() -> "MQTTConfig":
        if MQTTConfig.__instance is not None:
            return MQTTConfig.__instance

        ContextLogger.sys_log(LogLevel.INFO, "[MQTTConfig] initializing...")

        MQTTConfig.__instance = MQTTConfig(
            load_environment_variable("MQTT_CLIENT_ID", error_on_none=True),
            load_environment_variable("MQTT_HOST", error_on_none=True),
            int(load_environment_variable("MQTT_PORT", error_on_none=True)),
        )

        ContextLogger.sys_log(LogLevel.INFO, "[MQTTConfig] initialized.")
