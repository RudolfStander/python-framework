from typing import Any, Dict, Union
from db.daos.example import ExampleRecord


class ExampleObject:

    id: str
    tmstamp: str
    last_updated: Union[str, None]

    def __init__(self, id: str, tmstamp: str, last_updated: str = None):
        self.id = id
        self.tmstamp = tmstamp
        self.last_updated = last_updated

    @staticmethod
    def init_from_record(record: ExampleRecord) -> "ExampleObject":
        return ExampleObject(
            record.id,
            record.tmstamp,
            record.last_updated,
        )

    def to_record(self) -> ExampleRecord:
        return ExampleRecord.init(
            id=self.id,
            tmstamp=self.tmstamp,
            lastupdated=self.last_updated,
        )

    def to_json(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "tmstamp": self.tmstamp,
            "lastUpdated": self.last_updated,
        }
