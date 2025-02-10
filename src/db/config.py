from copy import deepcopy
from typing import Any, Dict


class DBConfig(object):
    host: str
    port: int
    database_name: str
    username: str
    password: str
    schema_id: str
    use_raw_schema_id: bool
    use_redshift: bool
    disable_ssl: bool

    def __init__(
        self,
        host: str,
        port: int,
        database_name: str,
        username: str,
        password: str,
        schema_id: str = "public",
        use_raw_schema_id: bool = True,
        use_redshift: bool = False,
        disable_ssl: bool = False,
    ):
        self.host = host
        self.port = port
        self.database_name = database_name
        self.username = username
        self.password = password
        self.schema_id = schema_id
        self.use_raw_schema_id = use_raw_schema_id
        self.use_redshift = use_redshift
        self.disable_ssl = disable_ssl

    def __repr__(self):
        repr_dict = deepcopy(self.__dict__)
        repr_dict["password"] = "******"

        return str(repr_dict)

    @staticmethod
    def from_json(obj: Dict[str, Any]) -> "DBConfig":
        return DBConfig(
            obj["host"],
            obj["port"],
            obj["database"],
            obj["username"],
            obj["password"],
            "public" if "schemaId" not in obj else obj["schemaId"],
            True if "useRawSchemaId" not in obj else obj["useRawSchemaId"],
            False if "useRedshift" not in obj else obj["useRedshift"],
            False if "disableSsl" not in obj else obj["disableSsl"],
        )
