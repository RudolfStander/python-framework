import traceback
from sys import exc_info, stdout
from time import sleep
from types import ModuleType
from typing import Any, Dict

from paho.mqtt.client import Client, MQTTMessage

from python_framework.config_utils import load_environment_variable
from python_framework.dynamic_loader import load_submodules
from python_framework.logger import ContextLogger, LogLevel
from python_framework.mqtt.mqtt_client import MQTTClient, mqtt_message_sanitized_payload
from python_framework.mqtt.mqtt_topic_handler import MQTTTopicHandler
from python_framework.thread_safe_cache import ThreadSafeCache
from python_framework.thread_safe_list import ThreadSafeList

LOGGER_KEY = "MQTTManager"
TRACE_LOGGING_ENABLED = (
    load_environment_variable("MQTT_TRACE_LOGGING", default="false").lower() == "true"
)


class MQTTManager:
    initialized = False
    default_client: MQTTClient = None
    handlers_by_topic: ThreadSafeCache[str, MQTTTopicHandler] = None
    active_subscriptions: ThreadSafeList[str]

    def __init__(self) -> None:
        pass

    @staticmethod
    def instance() -> "MQTTManager":
        return MQTTManager

    @staticmethod
    def initialize(handlers_module: ModuleType) -> "MQTTManager":
        if MQTTManager.is_initialized():
            return MQTTManager

        ContextLogger.instance().create_logger_for_context(
            LOGGER_KEY, LogLevel.TRACE if TRACE_LOGGING_ENABLED else LogLevel.DEBUG
        )
        ContextLogger.info(LOGGER_KEY, "initializing...")

        MQTTManager.default_client = MQTTClient(
            message_router, on_subscribe=on_subscribe, on_unsubscribe=on_unsubscribe
        )
        MQTTManager.handlers_by_topic = ThreadSafeCache()
        MQTTManager.active_subscriptions = ThreadSafeList()

        # need to set this here else we lose messages on client start
        MQTTManager.initialized = True

        MQTTManager.default_client.start()
        MQTTManager.default_client.wait_for_connection()

        handlers = load_submodules(handlers_module, ["register"])

        for handler in handlers:
            handler.register(MQTTManager)

        # after starting, we process the network events and confirm SUBACK
        for handler in MQTTManager.handlers_by_topic.values():
            MQTTManager.confirm_subscription(handler._rx_topic)

        ContextLogger.info(LOGGER_KEY, "initialized.")

    @staticmethod
    def stop(timeout=30) -> bool:
        MQTTManager.default_client.kill()

        try:
            MQTTManager.default_client.join(timeout=timeout)
        except:
            return False

        return True

    @staticmethod
    def is_initialized() -> bool:
        return MQTTManager.initialized

    @staticmethod
    def get_handler(topic: str) -> MQTTTopicHandler:
        if topic not in MQTTManager.handlers_by_topic:
            return None

        return MQTTManager.handlers_by_topic[topic]

    @staticmethod
    def register_handler(
        rx_topic: str, handler: MQTTTopicHandler, auto_start: bool = True
    ) -> bool:
        if rx_topic in MQTTManager.handlers_by_topic:
            ContextLogger.warn(
                LOGGER_KEY, f"topic [{rx_topic}] already bound to handler"
            )

            return False

        MQTTManager.handlers_by_topic[rx_topic] = handler

        if auto_start:
            handler.start()

        return True

    @staticmethod
    def remove_handler(rx_topic: str, wait: bool = True):
        if rx_topic not in MQTTManager.handlers_by_topic:
            return

        handler = MQTTManager.handlers_by_topic[rx_topic]
        del MQTTManager.handlers_by_topic[rx_topic]

        handler.stop()

    @staticmethod
    def get_topic_by_mid(mid: int) -> str:
        for topic in MQTTManager.handlers_by_topic.values():
            if mid == topic.mqtt_mid_value:
                return topic._rx_topic

        return None

    # TODO: add timeout / retry
    @staticmethod
    def confirm_subscription(topic: str) -> bool:
        if topic not in MQTTManager.handlers_by_topic:
            return False

        handler = MQTTManager.handlers_by_topic[topic]

        while True:
            ContextLogger.debug(
                LOGGER_KEY,
                "confirm_subscription: handler.mqtt_mid_value = [%d], topic active = [%s]"
                % (handler.mqtt_mid_value, (topic in MQTTManager.active_subscriptions)),
            )
            if (
                handler.mqtt_mid_value != -1
                and topic in MQTTManager.active_subscriptions
            ):
                return True

            sleep(0.1)


def message_router(client: Client, userdata: Dict[str, Any], message: MQTTMessage):
    try:
        ContextLogger.trace(
            LOGGER_KEY,
            f"received message: userdata = [{userdata}], topic = [{message.topic}], message payload = [{mqtt_message_sanitized_payload(message)}]",
        )
    except:
        ContextLogger.error(
            LOGGER_KEY,
            "failed to print message router info, error = [%s]" % repr(exc_info()),
        )
        traceback.print_exc(file=stdout)

    if message.topic is None or len(message.topic) == 0:
        ContextLogger.error(
            LOGGER_KEY, "invalid message received - null/empty [message.topic] field"
        )

        return

    try:
        if not MQTTManager.is_initialized():
            ContextLogger.warn(LOGGER_KEY, "MQTTManager not initialized yet")

            return

        handler = MQTTManager.get_handler(message.topic)

        if handler is None:
            ContextLogger.warn(LOGGER_KEY, "no handler for topic [%s]" % message.topic)

            return

        ContextLogger.trace(LOGGER_KEY, "routing message to topic [%s]" % message.topic)
        handler.on_message(userdata, message)
    except:
        ContextLogger.error(
            LOGGER_KEY, "failed to route message, error = [%s]" % repr(exc_info())
        )
        traceback.print_exc(file=stdout)


def on_subscribe(client, userdata, mid, granted_qos):
    topic_by_mid = MQTTManager.get_topic_by_mid(mid)

    ContextLogger.trace(
        LOGGER_KEY,
        "on_subscribe: mid = [%d], topic_by_mid = [%s]" % (mid, topic_by_mid),
    )

    if topic_by_mid is None:
        return

    if topic_by_mid not in MQTTManager.active_subscriptions:
        MQTTManager.active_subscriptions.append(topic_by_mid)

        ContextLogger.debug(
            LOGGER_KEY, "successfully subscribed to topic [%s]" % topic_by_mid
        )


def on_unsubscribe(client, userdata, mid, granted_qos):
    topic_by_mid = MQTTManager.get_topic_by_mid(mid)

    ContextLogger.trace(
        LOGGER_KEY,
        "on_unsubscribe: mid = [%d], topic_by_mid = [%s]" % (mid, topic_by_mid),
    )

    if topic_by_mid is None:
        return

    if topic_by_mid in MQTTManager.active_subscriptions:
        MQTTManager.active_subscriptions.remove(topic_by_mid)

        ContextLogger.debug(
            LOGGER_KEY, "successfully unsubscribed from topic [%s]" % topic_by_mid
        )
