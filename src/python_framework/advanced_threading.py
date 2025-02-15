from threading import Event, Lock, Thread
from typing import Any, Callable, List

from python_framework.logger import ContextLogger, LogLevel


def synchronized(func):
    func.__lock__ = Lock()

    def synced_func(*args, **kwargs):
        with func.__lock__:
            return func(*args, **kwargs)

    return synced_func


def synchronized_method(method):
    outer_lock = Lock()
    lock_name = "__%s_lock__" % method.__name__

    def wrapper_synchronized_method(*args, **kwargs):
        _self = args[0]

        with outer_lock:
            if not hasattr(_self, lock_name):
                setattr(_self, lock_name, Lock())

            lock = getattr(_self, lock_name)

            with lock:
                return method(*args, **kwargs)

    return wrapper_synchronized_method


# TODO: add callable as well
# TODO: register somewhere so it can be killed
class DelayedInvocation(Thread):

    object_reference: Any
    method_reference: str
    arguments: List[Any]
    delay: int

    _kill: Event
    force_stopped: bool

    def __init__(
        self,
        object_reference: Any,
        method_reference: str,
        delay: int,
        arguments: List[Any] = None,
    ):
        Thread.__init__(self)

        self.force_stopped = False
        self._kill = Event()

        self.object_reference = object_reference
        self.method_reference = method_reference
        self.delay = delay
        self.arguments = arguments

    def kill(self):
        self.force_stopped = True
        self._kill.set()

    def wait_or_kill(self, wait_time: int):
        return self._kill.wait(wait_time)

    @staticmethod
    def execute(
        object_reference: Any,
        method_reference: str,
        delay: int,
        arguments: List[Any] = None,
    ) -> "DelayedInvocation":
        _instance = DelayedInvocation(
            object_reference, method_reference, delay, arguments=arguments
        )

        _instance.start()

        return _instance

    def _invoke(self):
        ContextLogger.sys_log(
            LogLevel.INFO,
            "[DelayedInvocation] [%s.%s] invoking..."
            % (repr(self.object_reference), repr(self.method_reference)),
        )

        if not hasattr(self.object_reference, self.method_reference):
            ContextLogger.sys_log(
                LogLevel.ERROR,
                "[DelayedInvocation] [%s.%s] method does not exist."
                % (repr(self.object_reference), repr(self.method_reference)),
            )
            return

        func = getattr(self.object_reference, self.method_reference)

        if self.arguments is None:
            func()
        else:
            func(*self.arguments)

    def run(self):
        ContextLogger.sys_log(
            LogLevel.INFO,
            "[DelayedInvocation] [%s.%s] starting delay of []s..."
            % (repr(self.object_reference), repr(self.method_reference)),
        )

        if self.wait_or_kill(self.delay):
            return

        self._invoke()
