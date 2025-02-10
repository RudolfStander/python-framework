from sqlalchemy.engine.base import Connection

from db.config import DBConfig
from db.connection_pool import ConnectionPool, WaitableConnection
from db.postgresutils import ConnectionDetails


class TransactionManager(object):

    _connection_details: ConnectionDetails
    _waitable_connection: WaitableConnection

    def __init__(
        self, database_config: DBConfig = None, connection_details: ConnectionDetails = None
    ) -> None:
        if connection_details is None:
            self._connection_details = ConnectionDetails.from_db_config(database_config)
        else:
            self._connection_details = connection_details

        self._waitable_connection = None

    def __enter__(self) -> Connection:
        self._waitable_connection = ConnectionPool.get_pooled_connection(self._connection_details)

        if self._waitable_connection is None:
            raise Exception("Failed to start transaction - could not get connection from pool")

        try:
            connection = self._waitable_connection.start_transaction()
        except Exception:
            raise Exception("Failed to start transaction - could not")

        return connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self._waitable_connection.rollback_transaction()

            return False
        else:
            self._waitable_connection.commit_transaction()

            return True
