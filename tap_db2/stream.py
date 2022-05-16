import singer
import sqlalchemy

# from singer.catalog import Catalog, CatalogEntry

# from singer.schema import Schema
from singer_sdk.streams import SQLStream

LOGGER = singer.get_logger()


class StreamDb2(SQLStream):
    """
    Stream class for Db2 streams.
    """

    @classmethod
    def get_sqlalchemy_engine(cls, tap_config: dict) -> str:
        connection_string = "ibm_db_sa+pyodbc://{}:{}@{}:{}/{}".format(
            tap_config["username"],
            tap_config["password"],
            tap_config["hostname"],
            tap_config["port"],
            tap_config["database"],
        )

        return sqlalchemy.create_engine(connection_string)

    @classmethod
    def run_discovery(cls, tap_config):
        """
        Return discovered streams
        """

        LOGGER.info("Creating engine connection...")
        engine = cls.get_sqlalchemy_engine(tap_config)
        with engine.connect() as connection:
            LOGGER.info("Connected. Fetching database objects...")
            result_objects = connection.execute(
                """
                SELECT
                    TABSCHEMA AS TABLE_SCHEMA,
                    TABNAME AS TABLE_NAME,
                    TYPE AS TABLE_TYPE
                FROM SYSCAT.TABLES t
                WHERE t.TABSCHEMA NOT IN (
                    'SYSTOOLS',
                    'SYSIBM',
                    'SYSCAT',
                    'SYSPUBLIC',
                    'SYSSTAT',
                    'SYSIBMADM'
                )
                """
            )

            objects = {}
            for (
                schema,
                object_name,
                object_type,
            ) in result_objects.fetchall():
                if schema not in objects:
                    objects[schema] = {}
                objects[schema][object_name] = {
                    "type": "view" if object_type == "V" else "table",
                    "stream": object_name,
                    "tap_stream_id": object_name,
                    "properties": {}
                    # "schema": schema,
                }

            LOGGER.info(objects)
            LOGGER.info("Tables and views retrieved. Fetching columns...")

            result_columns = connection.execute(
                """
                SELECT
                    t.TABSCHEMA AS schema,
                    t.TABNAME AS OBJECT_NAME,
                    s.NAME AS column,
                    s.COLTYPE AS DATA_TYPE,
                    s.LENGTH AS CHARACTER_MAXIMUM_LENGTH,
                    s.LONGLENGTH AS NUMERIC_POSITION,
                    s.SCALE AS NUMERIC_SCALE,
                    CASE
                        WHEN s.KEYSEQ IS NOT NULL THEN 1
                        ELSE 0
                    END AS IS_PRIMARY_KEY
                FROM SYSIBM.SYSCOLUMNS s
                LEFT JOIN SYSCAT.TABLES t
                ON s.TBNAME = t.TABNAME
                WHERE t.TABSCHEMA NOT LIKE 'SYS%'
                """
            )

            for (
                schema,
                object_name,
                column,
                data_type,
                character_maximum_length,
                numeric_position,
                numeric_scale,
                is_primary_key,
            ) in result_columns.fetchall():
                LOGGER.info(f"objects: {objects}")
                if column not in objects[schema][object_name]["properties"]:
                    objects[schema][object_name]["properties"][column] = {}
                objects[schema][object_name]["properties"][column] = {
                    "type": data_type,
                    "character_maximum_length": character_maximum_length,
                    "numeric_position": numeric_position,
                    "numeric_scale": numeric_scale,
                    "is_primary_key": is_primary_key,
                }

            LOGGER.info(f"objects: {objects}")
            LOGGER.info("Fetched columns. Forming schema...")

            for (schema, stream) in objects.items():
                for (name, info) in stream.items():
                    LOGGER.info(f"{name}: {info}")
                    stream = {
                        "schema": {
                            "type": ["null", "object"],
                            "additionalProperties": False,
                        }
                    }

            # TO-DO
            return objects

            # stream = {
            #     "tap_stream_id": object_name,
            #     "stream": object_name,
            #     "schema": {
            #         "type": ["null", "object"],
            #         "additionalProperties": False,
            #         "properties": objects[schema][object_name][
            #             "properties"
            #         ],
            #         # {
            #         #     "TO-DO: id": {
            #         #         "type": ["null", "string"],
            #         #     },
            #         #     "TO-DO: date_modified": {
            #         #         "type": ["null", "string"],
            #         #         "format": "date-time",
            #         #     },
            #         # },
            #     },
            #     "metadata": [
            #         {
            #             "metadata": {
            #                 "inclusion": "available",
            #                 "table-key-properties": ["id"],
            #                 "selected": True,
            #                 "valid-replication-keys": ["date_modified"],
            #                 "schema-name": "users",
            #             },
            #             "breadcrumb": [],
            #         },
            #         {
            #             "metadata": {"inclusion": "automatic"},
            #             "breadcrumb": ["properties", "id"],
            #         },
            #         {
            #             "metadata": {
            #                 "inclusion": "available",
            #                 "selected": True,
            #             },
            #             "breadcrumb": ["properties", "name"],
            #         },
            #         {
            #             "metadata": {"inclusion": "automatic"},
            #             "breadcrumb": ["properties", "date_modified"],
            #         },
            #     ],
            # }

            # streams[object_name] = stream

            # catalog_entries["streams"].append(
            #     {
            #         "tap_stream_id": None,
            #         "stream": None,
            #         "key_properties": None,
            #         "schema": None,
            #         "replication_key": None,
            #         "replication_method": None,
            #         "is_view": None,
            #         "database": None,
            #         "table": None,
            #         "row_count": None,
            #         "stream_alias": None,
            #         "metadata": None,
            #     }
            # )

            # output = {"streams": list(streams.values())}
            # LOGGER.info(f"output = {output}")
            # return output
