from enum import Enum
from typing import Dict, List, Union

import python_framework.db.dao.dao as BaseDAO
from python_framework.db.dao.objects import DAOQuery, DAORecord
from python_framework.time import timestamp_to_utc_timestamp


class ExampleQuery(Enum):
    UPDATE_CUSTOM = "UPDATE_CUSTOM"


class ExampleRecord(DAORecord):
    id: str
    tmstamp: str
    last_updated: str

    def __init__(self, result: dict):
        super().__init__(result)

        self.id = result["id"]
        self.tmstamp = (
            None
            if result["tmstamp"] is None
            else timestamp_to_utc_timestamp(result["tmstamp"])
        )
        self.last_updated = (
            None
            if result["lastupdated"] is None
            else timestamp_to_utc_timestamp(result["lastupdated"])
        )

    def generate_insert_query_args(self) -> Dict[str, Union[str, int, bool, float]]:
        return {
            "id": self.id,
            "tmstamp": self.tmstamp,
        }

    def generate_update_query_args(self) -> Dict[str, Union[str, int, bool, float]]:
        return {
            "id": self.id,
            "tmstamp": self.tmstamp,
            "expected_last_updated": self.last_updated,
        }

    def generate_upsert_query_args(self) -> Dict[str, Union[str, int, bool, float]]:
        return super().generate_upsert_query_args()

    def generate_delete_query_args(self) -> Dict[str, Union[str, int, bool, float]]:
        return {"id": self.id}


class ExampleSelectAllQuery(DAOQuery):
    def __init__(self):
        super().__init__(ExampleRecord)

    def to_sql(self):
        field_map = {}

        sql = """
            SELECT
                Id,
                TMStamp::text,
                LastUpdated::text
            FROM Example
        """

        return sql, field_map


class ExampleInsertQuery(DAOQuery):
    def __init__(self, id: str, tmstamp: str):
        super().__init__(ExampleRecord)

        self.id = id
        self.tmstamp = tmstamp

    def to_sql(self):
        field_map = {
            "query_Id": self.id,
            "query_TMStamp": self.tmstamp,
        }

        sql = """
            INSERT INTO Example (
                Id,
                TMStamp,
                LastUpdated
            )
            VALUES (
                :query_Id,
                :query_TMStamp,
                CURRENT_TIMESTAMP
            )
            RETURNING
                Id,
                TMStamp::text,
                LastUpdated::text
        """

        return sql, field_map


class ExampleUpdateQuery(DAOQuery):
    def __init__(
        self,
        id: str,
        tmstamp: str,
        expected_last_updated: str,
    ):
        super().__init__(ExampleRecord)

        self.id = id
        self.tmstamp = tmstamp
        self.expected_last_updated = expected_last_updated

    def to_sql(self):
        field_map = {
            "query_Id": self.id,
            "query_TMStamp": self.tmstamp,
            "query_ExpectedLastUpdated": self.expected_last_updated,
        }

        sql = """
            UPDATE Example 
            SET
                TMStamp = :query_TMStamp,
                LastUpdated = CURRENT_TIMESTAMP
            WHERE Id = :query_Id
            AND LastUpdated = :query_ExpectedLastUpdated
            RETURNING
                Id,
                TMStamp::text,
                LastUpdated::text
        """

        return sql, field_map


class ExampleDeleteQuery(DAOQuery):
    def __init__(self, id: str):
        super().__init__(ExampleRecord)

        self.id = id

    def to_sql(self):
        field_map = {"query_Id": self.id}

        sql = """
            DELETE FROM Example 
            WHERE Id = :query_Id
            RETURNING
                Id,
                TMStamp::text,
                LastUpdated::text
        """

        return sql, field_map


class ExampleUpdateCustomQuery(DAOQuery):
    def __init__(self, id: str):
        super().__init__(ExampleRecord)

        self.id = id

    def to_sql(self):
        field_map = {
            "query_Id": self.id,
        }

        sql = """
            UPDATE Example 
            SET
                TMStamp = CURRENT_TIMESTAMP
            WHERE Id = :query_Id
            RETURNING
                Id,
                TMStamp::text,
                LastUpdated::text
        """

        return sql, field_map


class ExampleDAO(BaseDAO.DAO):
    queries = {
        BaseDAO.SELECT_ALL_QUERY_KEY: ExampleSelectAllQuery,
        BaseDAO.INSERT_QUERY_KEY: ExampleInsertQuery,
        BaseDAO.UPDATE_QUERY_KEY: ExampleUpdateQuery,
        BaseDAO.DELETE_QUERY_KEY: ExampleDeleteQuery,
        ExampleQuery.UPDATE_CUSTOM: ExampleUpdateCustomQuery,
    }
