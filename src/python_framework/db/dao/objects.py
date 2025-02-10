from abc import ABC, abstractmethod
from typing import Dict, Tuple, Type, Union


class DAORecord(ABC):
    @abstractmethod
    def __init__(self, result: dict) -> Type["DAORecord"]:
        pass

    @classmethod
    def init(cls, **kwargs) -> Type["DAORecord"]:
        return cls(result=kwargs)

    def __repr__(self) -> str:
        return str(self.__dict__)

    @abstractmethod
    def generate_insert_query_args(self) -> Dict[str, Union[str, int, bool, float]]:
        return {}

    @abstractmethod
    def generate_update_query_args(self) -> Dict[str, Union[str, int, bool, float]]:
        return {}

    @abstractmethod
    def generate_upsert_query_args(self) -> Dict[str, Union[str, int, bool, float]]:
        return {}

    @abstractmethod
    def generate_delete_query_args(self) -> Dict[str, Union[str, int, bool, float]]:
        return {}


class DAOQuery(ABC):

    record_constructor: Type[DAORecord]

    @abstractmethod
    def __init__(self, record_constructor: Type[DAORecord]) -> Type["DAOQuery"]:
        self.record_constructor = record_constructor

    def __repr__(self) -> str:
        return str(self.__dict__)

    @abstractmethod
    def to_sql(self) -> Tuple[str, Dict]:
        field_map = {}
        sql = ""

        return sql, field_map

    def map_result(self, result) -> Type[DAORecord]:
        if self.record_constructor is None:
            return None

        return self.record_constructor(result)
