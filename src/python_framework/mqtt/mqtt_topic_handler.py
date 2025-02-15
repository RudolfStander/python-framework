from typing import Any, Dict

from paho.mqtt.client import MQTTMessage

from python_framework.config_utils import load_environment_variable
from python_framework.logger import ContextLogger, LogLevel
from python_framework.mqtt.mqtt_client import MQTTClient

TRACE_LOGGING_ENABLED = (
    load_environment_variable("MQTT_TRACE_LOGGING", default="false").lower() == "true"
)


class MQTTTopicHandler:

    _client: MQTTClient
    _tx_topic: str
    _rx_topic: str
    _qos: int
    _handler_classname: str

    _logger_key: str

    mqtt_mid_value: int

    def __init__(
        self,
        client: MQTTClient,
        tx_topic: str,
        rx_topic: str,
        qos: int = 2,
        handler_classname: str = "MQTTTopicHandler",
    ) -> None:
        self._client = client
        self._tx_topic = tx_topic
        self._rx_topic = rx_topic
        self._qos = qos

        self._handler_classname = handler_classname
        self.mqtt_mid_value = -1

        self._logger_key = f"MQTT@ {self._client._client_id}_{handler_classname}"
        ContextLogger.instance().create_logger_for_context(
            self._logger_key,
            LogLevel.TRACE if TRACE_LOGGING_ENABLED else LogLevel.DEBUG,
        )

    def start(self):
        sub_result = self._client.subscribe(self._rx_topic, qos=self._qos)
        self.mqtt_mid_value = sub_result[1]

    def stop(self):
        self._client.unsubscribe(self._rx_topic)
        self.mqtt_mid_value = -1

    # this will be overriden in implementing subscribers
    def on_message(self, userdata: Dict[str, Any], message: MQTTMessage):
        pass

    def publish(self, message: str):
        ContextLogger.trace(
            self._logger_key,
            "publishing message [%s] to topic [%s]" % (message, self._tx_topic),
        )
        self._client.publish(self._tx_topic, message, self._qos)
