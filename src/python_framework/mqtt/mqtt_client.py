import traceback
from copy import deepcopy
from json import loads
from sys import exc_info, stdout
from threading import Event, Thread
from typing import Any, Callable, Dict, List

from paho.mqtt.client import Client, MQTTMessage, MQTTv5, MQTTv311

from python_framework.mqtt.mqtt_config import MQTTConfig
from python_framework.config_utils import load_environment_variable
from python_framework.logger import ContextLogger, LogLevel

LOGGER_KEY = "MQTTClient"
TRACE_LOGGING_ENABLED = (
    load_environment_variable("MQTT_TRACE_LOGGING", default="false").lower() == "true"
)

MessageRouter = Callable[[Client, Dict[str, Any], MQTTMessage], None]


def mqtt_message_sanitized_payload(message: MQTTMessage, message_end_token="$") -> str:
    payload = str(message.payload.decode("utf-8", errors="ignore"))

    if message_end_token not in payload:
        return payload

    return payload.split(message_end_token)[0]


def mqtt_message_json_payload(message: MQTTMessage) -> Dict[str, Any]:
    try:
        return loads(message.payload)
    except:
        ContextLogger.error(
            LOGGER_KEY,
            "failed to parse MQTT payload as json, error = [%s]" % repr(exc_info()),
        )
        traceback.print_exc(file=stdout)

    return None


def on_log(client, userdata, level, buf):
    ContextLogger.trace(LOGGER_KEY, "log: [%s]" % buf)


class MQTTClient(Thread):
    _kill: Event
    force_stopped: bool

    _client: Client
    _message_router: MessageRouter
    _connection_initiated: bool

    _client_id: str
    _host: str
    _port: int
    _mqtt_userdata: Dict[str, Any]
    _subscriptions: List[str]

    def __init__(
        self,
        message_router: MessageRouter,
        mqtt_userdata: Dict[str, Any] = None,
        client_id: str = None,
        host: str = None,
        port: int = None,
        protocol: int = MQTTv311,
        on_subscribe: Any = None,
        on_unsubscribe: Any = None,
    ) -> None:
        Thread.__init__(self)

        self.force_stopped = False
        self._kill = Event()

        self._message_router = message_router
        self._client_id = (
            client_id if client_id is not None else MQTTConfig.instance().client_id
        )
        self._host = host if host is not None else MQTTConfig.instance().mqtt_host
        self._port = port if port is not None else MQTTConfig.instance().mqtt_port

        if mqtt_userdata is None:
            self._mqtt_userdata = {"client_id": self._client_id}
        else:
            self._mqtt_userdata = deepcopy(mqtt_userdata)
            self._mqtt_userdata["client_id"] = self._client_id

        if protocol == MQTTv5:
            self._client = Client(
                client_id=self._client_id,
                protocol=protocol,
                userdata=self._mqtt_userdata,
            )
        else:
            self._client = Client(
                client_id=self._client_id,
                protocol=protocol,
                userdata=self._mqtt_userdata,
                # clean_session=False,
            )

        self._connection_initiated = False

        ContextLogger.instance().create_logger_for_context(
            LOGGER_KEY, LogLevel.TRACE if TRACE_LOGGING_ENABLED else LogLevel.DEBUG
        )

        self._client.on_message = self._message_router
        self._client.on_log = on_log
        self._client.on_subscribe = on_subscribe
        self._client.on_unsubscribe = on_unsubscribe

    def kill(self):
        self.force_stopped = True
        self._kill.set()

    def wait_or_kill(self, wait_time: int = 60):
        return self._kill.wait(wait_time)

    def connect(self, force=False):
        ContextLogger.info(LOGGER_KEY, "connecting to broker...")

        if not force and self._client.is_connected():
            ContextLogger.info(LOGGER_KEY, "already connected.")
            return

        if not force and self._connection_initiated:
            ContextLogger.info(LOGGER_KEY, "connect was already called.")
            return

        self._client.connect(self._host, port=self._port)
        self._connection_initiated = True

    # TODO: add timeout
    def wait_for_connection(self):
        while not self._client.is_connected():
            if self.wait_or_kill(1):
                return

    def subscribe(self, topic: str, qos: int) -> Any:
        ContextLogger.info(LOGGER_KEY, "subscribing to [%s @ qos=%d]" % (topic, qos))
        return self._client.subscribe(topic, qos=qos)

    def unsubscribe(self, topic: str):
        ContextLogger.info(LOGGER_KEY, "unsubscribing from [%s]" % topic)
        return self._client.unsubscribe(topic)

    def publish(self, topic: str, message: str, qos: int):
        self._client.publish(topic, message, qos=qos)

    def _execute_in_loop(self):
        pass

    def _reconnect(self):
        while True:
            try:
                if self._client.is_connected():
                    return

                ContextLogger.debug(LOGGER_KEY, "reconnecting...")
                self._client.reconnect()

                if self._client.is_connected():
                    return
            except:
                ContextLogger.error(
                    LOGGER_KEY, "reconnect failed, error = [%s]" % repr(exc_info())
                )

            if self.wait_or_kill(wait_time=1):
                return

    def run(self):
        ContextLogger.info(LOGGER_KEY, "starting client...")

        self.connect()

        ContextLogger.info(LOGGER_KEY, "starting loop...")
        self._client.loop_start()

        while True:
            try:
                if self.wait_or_kill():
                    break

                self._execute_in_loop()

                if not self._client.is_connected():
                    self._reconnect()
            except:
                ContextLogger.error(
                    LOGGER_KEY,
                    "execution failure in loop, error = [%s]" % repr(exc_info()),
                )
                traceback.print_exc(file=stdout)

        ContextLogger.info(LOGGER_KEY, "stopping loop...")
        self._client.loop_stop()

        ContextLogger.info(LOGGER_KEY, "handler stopped.")
