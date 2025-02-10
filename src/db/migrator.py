import re
from functools import cmp_to_key
from hashlib import sha256
from os import listdir

from sqlalchemy import text

from db.config import DBConfig
from db.postgresutils import ConnectionDetails, create_transaction

MIGRATION_ID_REGEX = re.compile("V((\d)+_(\d)+)__.*")


class Migrator(object):
    def __init__(self, migrations_path: str, service_db_config: DBConfig):
        self.migrations_path = migrations_path
        self.service_db_config = service_db_config
        self.migrations = {}

    def migrate(self):
        if self.service_db_config is None:
            print("WARNING - Could not perform migrations. Service Context is not configured.")
            return False

        try:
            print("MIGRATOR - Ensuring schema exists...")
            self.ensure_schema_exists()
        except Exception as e:
            print("ERROR - Failed to ensure schema exists:\n", e)
            return False

        try:
            print("MIGRATOR - Loading migrations...")
            self.load_migrations()
        except Exception as e:
            print("ERROR - Failed to load migrations:\n", e)
            return False

        try:
            print("MIGRATOR - Validating migrations...")
            self.validate_migrations()
        except Exception as e:
            print("ERROR - Validation failed:\n", e)
            return False

        try:
            print("MIGRATOR - Executing migrations...")
            self.execute_migrations()
        except Exception as e:
            print("ERROR - Failed to execute migrations:\n", e)
            return False

        return True

    def ensure_schema_exists(self):
        query = MigratorDDLQuery(
            "CREATE SCHEMA IF NOT EXISTS %s;" % self.service_db_config.schema_id
        )

        execute_query(query, self.service_db_config, return_count_only=True)

    def load_migrations(self):
        filenames = listdir(self.migrations_path)
        migrations = []
        migration_ids = []

        for filename in filenames:
            id_search = MIGRATION_ID_REGEX.search(filename)

            if id_search is None or id_search.group(1) is None or len(id_search.group(1)) < 3:
                raise Exception(
                    "Invalid filename for migration. Could not determine migration ID: %s"
                    % filename
                )

            id = id_search.group(1)

            if id in migration_ids:
                raise Exception("Duplicate migration ID: %s" % id)

            migration = {"id": id, "filename": filename}

            migration_ids.append(id)
            migrations.append(migration)

        sorted_migrations = sort_migrations(migrations)

        for migration in sorted_migrations:
            with open("%s/%s" % (self.migrations_path, migration["filename"]), "r") as file:
                contents = file.read()
                checksum = sha256(contents.encode("UTF-8")).hexdigest()

                migration["checksum"] = checksum
                migration["insert_query"] = MigrationInsertQuery(
                    migration["id"], migration["filename"], checksum
                )
                migration["ddl_query"] = MigratorDDLQuery(contents)

            self.migrations[migration["id"]] = migration

            print("[%s - %s] - Migration loaded" % (migration["id"], migration["filename"]))

    def validate_migrations(self):
        load_query = MigrationLoadExistingEntriesQuery()
        load_results = []

        try:
            load_results = execute_query(load_query, self.service_db_config)
        except:
            # The migration table might not exist yet, so it's okay
            return

        for result in load_results:
            if result.id not in self.migrations:
                continue

            if result.migration_name != self.migrations[result.id]["filename"]:
                raise Exception(
                    "Migration name mismatch for [%s]\n    persisted name: %s\n    loaded name: %s"
                    % (result.id, result.migration_name, self.migrations[result.id]["filename"])
                )

            if result.checksum != self.migrations[result.id]["checksum"]:
                raise Exception(
                    "Migration checksum mismatch for [%s - %s]\n    persisted checksum: %s\n    loaded checksum: %s"
                    % (
                        result.id,
                        result.migration_name,
                        result.checksum,
                        self.migrations[result.id]["checksum"],
                    )
                )

            print(
                "[%s - %s] - Migration validated. Removing from migrations to be run."
                % (result.id, result.migration_name)
            )
            del self.migrations[result.id]

    def execute_migrations(self):
        sorted_migrations = sort_migrations(self.migrations.values())

        for migration in sorted_migrations:
            print(
                "Executing migration [%s - %s] for schema [%s]"
                % (migration["id"], migration["filename"], self.service_db_config.schema_id)
            )

            execute_query(migration["ddl_query"], self.service_db_config, return_count_only=True)
            insert_result = execute_query(migration["insert_query"], self.service_db_config)

            if insert_result is None or len(insert_result) == 0:
                raise Exception(
                    "Failed to insert migration into migrations table: [%s - %s]"
                    % (migration["id"], migration["filename"])
                )

            print("[%s - %s] - Migration executed" % (migration["id"], migration["filename"]))


def migration_sort_comparator(migration_1: dict, migration_2: dict):
    id_1_split = migration_1["id"].split("_")
    id_1_split[0] = int(id_1_split[0])
    id_1_split[1] = int(id_1_split[1])
    id_2_split = migration_2["id"].split("_")
    id_2_split[0] = int(id_2_split[0])
    id_2_split[1] = int(id_2_split[1])

    cmp = id_1_split[0] - id_2_split[0]

    if cmp != 0:
        return cmp

    return id_1_split[1] - id_2_split[1]


def sort_migrations(migrations: []):
    return sorted(migrations, key=cmp_to_key(migration_sort_comparator))


class MigrationEntry(object):
    def __init__(self, result):
        self.id = result["id"]
        self.migration_name = result["migrationname"]
        self.checksum = result["checksum"]
        self.tmstamp = result["tmstamp"]

    def __repr__(self):
        return str(self.__dict__)


class MigrationInsertQuery(object):
    def __init__(self, id: str, migration_name: str, checksum: str):
        self.id = id
        self.migration_name = migration_name
        self.checksum = checksum

    def __repr__(self):
        return str(self.__dict__)

    def to_sql(self):
        field_map = {
            "query_Id": self.id,
            "query_MigrationName": self.migration_name,
            "query_Checksum": self.checksum,
        }

        sql = """
        INSERT INTO Migration (
            Id,
            MigrationName,
            Checksum,
            TMStamp
        )
        VALUES (
            :query_Id,
            :query_MigrationName,
            :query_Checksum,
            CURRENT_TIMESTAMP
        )
        RETURNING 
            Id,
            MigrationName,
            Checksum,
            TMStamp::text
    """

        return sql, field_map

    def map_result(self, result):
        return MigrationEntry(result)


class MigrationLoadExistingEntriesQuery(object):
    def __init__(self):
        pass

    def __repr__(self):
        return str(self.__dict__)

    def to_sql(self):
        sql = """
        SElECT
            Id,
            MigrationName,
            Checksum,
            TMStamp::text
        FROM Migration
    """

        return sql, {}

    def map_result(self, result):
        return MigrationEntry(result)


class MigratorDDLQuery(object):
    def __init__(self, sql: str):
        self.sql = sql

    def __repr__(self):
        return str(self.__dict__)

    def to_sql(self):
        return self.sql, {}

    def map_result(self, result):
        return True


def execute_query(query, database_config: DBConfig, return_count_only=False):
    if database_config is None:
        print("WARNING - Could not perform migrations. Service Context is not configured.")
        return 0 if return_count_only else []

    sql, field_map = query.to_sql()

    connection_details = ConnectionDetails.from_db_config(database_config)

    with create_transaction(
        connection_details=connection_details, keep_connection_alive=True
    ) as transaction:
        results = transaction.execute(text(sql), field_map)

        if return_count_only:
            return results.rowcount

        if results is None or results.rowcount == 0:
            return []

        mapped_results = []

        for result in results:
            mapped_results.append(query.map_result(result))

        return mapped_results
