import traceback
from sys import exc_info, stdout
from threading import Lock
from typing import Any, List
from uuid import uuid4

from sqlalchemy import create_engine
from sqlalchemy.engine.base import Connection, Engine, Transaction
from sqlalchemy.pool import NullPool

from python_framework.config_utils import load_environment_variable
from python_framework.db.postgresutils import ConnectionDetails
from python_framework.logger import ContextLogger, LogLevel
from python_framework.thread_safe_cache import ThreadSafeCache
from python_framework.thread_safe_list import ThreadSafeList

LOGGER_KEY = "CONNECTION_POOL"

LOGGER_INITIALIZED = False


def initialize_logger():
    global LOGGER_INITIALIZED

    if LOGGER_INITIALIZED:
        return

    enable_trace_logging = load_environment_variable(
        "ENABLE_CONNECTION_POOL_TRACE_LOGGING", "False"
    )

    if enable_trace_logging.lower() == "true":
        ContextLogger.instance().create_logger_for_context(LOGGER_KEY, LogLevel.TRACE)
    else:
        ContextLogger.instance().create_logger_for_context(LOGGER_KEY, LogLevel.INFO)

    LOGGER_INITIALIZED = True


class WaitableConnection(object):
    _id: str
    _connection: Connection
    _lock: Lock
    _transaction: Transaction
    _in_use: bool
    _pool: "ConnectionPool"

    def __init__(self, connection: Connection, pool: "ConnectionPool") -> None:
        self._id = str(uuid4())
        self._connection = connection
        self._pool = pool
        self._lock = Lock()
        self._transaction = None
        self._in_use = False

    def is_healthy(self) -> bool:
        return self._connection is not None and not self._connection.closed

    def in_use(self) -> bool:
        return self._in_use

    def start_transaction(self) -> Connection:
        self._pool.log("acquiring lock on connection...", LogLevel.TRACE)
        self._lock.acquire()
        self._pool.log("lock acquired on connection...", LogLevel.TRACE)
        self._in_use = True

        # assess connection health, if unhealthy, close and create new connection
        if not self.is_healthy() and not self.self_heal():
            self._in_use = False
            self._lock.release()
            raise Exception(
                "Failed to start transaction - connection could not self-heal"
            )

        self._pool.log("creating transaction...", LogLevel.TRACE)

        self._transaction = self._connection.begin()

        self._pool.log("returning connection...", LogLevel.TRACE)

        return self._connection

    def __eq__(self, other: object) -> bool:
        return self.__class__ is other.__class__ and self._id == other._id

    # We assume this is already inside a locked region, so we do not lock again
    def self_heal(self) -> bool:
        self._pool.log("self-healing connection..." % repr(exc_info()), LogLevel.DEBUG)

        try:
            self._connection.close()
        except:
            # we don't care about a failure here, since we wanted the connection closed anyway
            pass

        try:
            self._connection = self._pool.create_connection(raw_connection_only=True)
            return True
        except:
            self._pool.log(
                "failed to self-heal connection. Removing connection from pool. Error = [%s]"
                % repr(exc_info()),
                LogLevel.ERROR,
            )
            traceback.print_exc(file=stdout)

        self._pool.remove_connection(self)

        return False

    def rollback_transaction(self, exc_val: Any = None):
        self._pool.log(
            "rolling back transaction. Exception = [%s]" % exc_val, LogLevel.ERROR
        )

        self._transaction.rollback()
        self._in_use = False
        self._lock.release()

    def commit_transaction(self):
        self._pool.log("committing transaction...", LogLevel.TRACE)

        self._transaction.commit()
        self._in_use = False
        self._lock.release()

    def close_transaction(self):
        self._pool.log("closing non transactional connection...", LogLevel.TRACE)

        self._in_use = False
        self._lock.release()
        self._connection.close()

    def __enter__(self) -> Connection:
        return self.start_transaction()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.rollback_transaction(exc_val)
            exc_tb.print_exc(file=stdout)

            return False
        else:
            self.commit_transaction()

            return True


class ConnectionPool:
    POOLS: ThreadSafeCache[str, "ConnectionPool"] = ThreadSafeCache()

    _engine: Engine
    _connection_details: ConnectionDetails
    _connections: ThreadSafeList[WaitableConnection]
    _connection_cycle: "ConnectionCycle"
    _lock: Lock
    _max_pool_size: int

    logger_key: str

    def __init__(
        self, connection_details: ConnectionDetails, max_pool_size: int
    ) -> None:
        self._engine = None
        self._connection_details = connection_details
        self._connections = ThreadSafeList()
        self._connection_cycle = ConnectionCycle(self)
        self._lock = Lock()
        self._max_pool_size = max_pool_size
        self.logger_key = (
            f"{LOGGER_KEY} - {connection_details.database}.{connection_details.schema}"
        )

        if (
            load_environment_variable("ENABLE_POSTGRES_TRACE_LOGGING", "False").lower()
            == "true"
        ):
            ContextLogger.instance().create_logger_for_context(
                self.logger_key, LogLevel.TRACE
            )
        else:
            ContextLogger.instance().create_logger_for_context(
                self.logger_key, LogLevel.INFO
            )

    def log(self, log_str: str, log_level: LogLevel = LogLevel.INFO):
        ContextLogger.log(self.logger_key, log_level, log_str)

    @staticmethod
    def instance(connection_details: ConnectionDetails) -> "ConnectionPool":
        if connection_details.get_key() in ConnectionPool.POOLS:
            return ConnectionPool.POOLS[connection_details.get_key()]

        return ConnectionPool.initialize(
            connection_details,
            max_pool_size=int(
                load_environment_variable("EXTERNAL_DB_POOL_MAX_SIZE", 10)
            ),
            initial_pool_size=int(
                load_environment_variable("EXTERNAL_DB_POOL_INITIAL_SIZE", 5)
            ),
        )

    @staticmethod
    def initialize(
        connection_details: ConnectionDetails,
        max_pool_size: int = 5,
        initial_pool_size: int = 5,
    ) -> "ConnectionPool":
        if connection_details.get_key() in ConnectionPool.POOLS:
            return ConnectionPool.POOLS[connection_details.get_key()]

        ContextLogger.debug(
            LOGGER_KEY,
            "creating pool with key = [%s], max_pool_size = [%d]..."
            % (connection_details.get_key(), max_pool_size),
        )

        pool = ConnectionPool(connection_details, max_pool_size)
        pool.create_db_engine()

        if initial_pool_size > 0:
            pool.log(
                "initialising pool with [%d] connections..." % initial_pool_size,
                LogLevel.DEBUG,
            )

            for i in range(initial_pool_size):
                pool.create_connection()

        ConnectionPool.POOLS[connection_details.get_key()] = pool

        pool.log(
            "pool created with key = [%s]." % connection_details.get_key(),
            LogLevel.DEBUG,
        )

        return pool

    @staticmethod
    def get_pooled_connection(
        connection_details: ConnectionDetails,
    ) -> WaitableConnection:
        pool = ConnectionPool.instance(connection_details)

        if pool is None:
            return None

        return pool.get_connection()

    def get_connection(self) -> WaitableConnection:
        connection: WaitableConnection = None

        self.log("getting connection...", LogLevel.TRACE)

        # print("cycle size = ", self._connection_cycle.size())
        # print("peek().in_use = ", self._connection_cycle.peek().in_use())

        if self._connection_cycle.size() == 0 or (
            # the pool is smaller than the max size
            self._connection_cycle.size() < self._max_pool_size
            # next connection in pool is in use
            and self._connection_cycle.peek().in_use()
            # the pool is not currently being scaled
            and not self._lock.locked()
        ):
            connection = self.create_connection(lock=True)
        else:
            connection = self._connection_cycle.cycle()

        self.log("returning connection.", LogLevel.TRACE)

        return connection

    def create_db_engine(self, recreate: bool = False) -> Engine:
        with self._lock:
            if self._engine is not None and not recreate:
                return self._engine

            self.log("creating DB engine...", LogLevel.TRACE)

            self._engine = create_engine(
                self._connection_details.get_connection_string(),
                connect_args=self._connection_details.get_connection_args(),
                poolclass=NullPool,
                # pool_size=self._max_pool_size,
                # pool_pre_ping=True,
            )

            if (
                self._connection_details.schema is not None
                and len(self._connection_details.schema) > 0
            ):
                self._engine.execution_options(
                    schema_translate_map={None: self._connection_details.schema}
                )

            self.log(
                "DB engine created, pool = [%s]" % (self._engine.pool), LogLevel.TRACE
            )

            return self._engine

    def _create_connection(self) -> WaitableConnection:
        # ensure pool not overflowing
        if self._connection_cycle.size() >= self._max_pool_size:
            return self._connection_cycle.cycle()

        self.log("creating new connection...", LogLevel.TRACE)

        connection = self._engine.connect()
        waitable_connection = WaitableConnection(connection, self)
        self._connections.append(waitable_connection)
        self._connection_cycle.add(waitable_connection)

        self.log(
            "new connection created, current pool size = [%d]"
            % self._connection_cycle.size(),
            LogLevel.TRACE,
        )

        return waitable_connection

    def create_connection(
        self, raw_connection_only: bool = False, lock: bool = False
    ) -> WaitableConnection:
        if self._engine is None:
            self.create_db_engine()

        if raw_connection_only:
            return self._engine.connect()

        if lock:
            with self._lock:
                return self._create_connection()
        else:
            return self._create_connection()

    def remove_connection(self, connection: WaitableConnection) -> bool:
        self.log(
            "removing connection with id = [%s] from pool..." % connection._id,
            LogLevel.DEBUG,
        )

        with self._lock:
            removed = self._connection_cycle.delete(connection)

            if removed:
                self.log(
                    "connection with id = [%s] removed from pool, current pool size = [%d]"
                    % (connection._id, self._connection_cycle.size()),
                    LogLevel.DEBUG,
                )
            else:
                self.log(
                    "failed to remove connection with id = [%s] from pool, current pool size = [%d]"
                    % (connection._id, self._connection_cycle.size()),
                    LogLevel.WARN,
                )

            return removed

    def size(self) -> int:
        return self._connection_cycle.size()


class AtomicInt(object):
    _value: int
    _lock: Lock

    def __init__(self, value: int = 0) -> None:
        self._value = value
        self._lock = Lock()

    def get(self) -> int:
        return self._value

    def set(self, value: int):
        with self._lock:
            self._value = value

    def inc(self):
        with self._lock:
            self._value += 1

    def dec(self):
        with self._lock:
            self._value -= 1

    # returns current value and updates the value for next operation
    def next(self, max: int):
        with self._lock:
            current = self._value

            if current + 1 > max:
                self._value = 0
            else:
                self._value += 1

            return current


class ConnectionCycle:
    _items_lock: Lock
    _items: List[WaitableConnection]
    _pointer: AtomicInt
    _pool: ConnectionPool

    def __init__(self, pool: ConnectionPool = None) -> None:
        self._items_lock = Lock()
        self._items = []
        self._pointer = AtomicInt()
        self._pool = pool

    def log(self, log_str: str, log_level: LogLevel = LogLevel.INFO):
        if self._pool is None:
            ContextLogger.log(LOGGER_KEY, log_level, log_str)
        else:
            self._pool.log(log_str, log_level)

    def size(self) -> int:
        return len(self._items)

    def add(self, connection: WaitableConnection) -> None:
        with self._items_lock:
            self._items.append(connection)

    def delete(self, connection: WaitableConnection):
        with self._items_lock:
            index = -1
            i = 0

            for c in self._items:
                if c == connection:
                    index = i
                    break

                i += 1

            if index == -1:
                return False

            self._delete_by_index(index)

            return True

    def _delete_by_index(self, index: int) -> WaitableConnection:
        if index >= len(self._items):
            return None

        # adjust pointer IFF it is at or past the index, if it's before, we can ignore it
        pointer = self._pointer.get()

        # if the pointer is at or past the index, we need to safely move it back
        if pointer >= index:
            # if pointer is at the max, i.e. it will be out of bounds after the delete, we set the pointer to 0
            if pointer >= len(self._items) - 1:
                self._pointer.set(0)
            else:  # else we decrement, since the pointer should move 1 back after the delete
                self._pointer.dec()

        conn = self._items[index]
        del self._items[index]

        return conn

    def delete_by_index(self, index: int) -> WaitableConnection:
        with self._items_lock:
            self._delete_by_index(index)

    def peek(self) -> WaitableConnection:
        if len(self._items) == 0:
            return None

        return self._items[self._pointer.get()]

    def _cycle(self) -> WaitableConnection:
        conn = self._items[self._pointer.next(len(self._items) - 1)]

        self.log(
            "connections cycled, current_pointer = [%d]..." % self._pointer.get(),
            LogLevel.TRACE,
        )

        return conn

    def cycle(self) -> WaitableConnection:
        if len(self._items) == 0:
            return None

        self.log(
            "cycling connections, current_pointer = [%d]..." % self._pointer.get(),
            LogLevel.TRACE,
        )

        # if there's a lock on items, we need to wait for it to be sure we're not indexing out of bounds
        if self._items_lock.locked():
            with self._items_lock:
                return self._cycle()
        else:
            return self._cycle()


class NoLock:
    def __init__(self) -> None:
        pass

    def acquire(self, blocking=True, timeout=-1):
        pass

    def release(self):
        pass

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
