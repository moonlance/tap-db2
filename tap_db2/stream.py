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
                objects[object_name] = {
                    "is_view": object_type == "V",
                    "schema": schema,
                }

            LOGGER.info(objects)
            LOGGER.info("Tables and views retrieved. Fetching columns...")

            result_columns = connection.execute(
                """
                SELECT
                    t.TABSCHEMA AS TABLE_SCHEMA,
                    t.TABNAME AS TABLE_NAME,
                    s.NAME AS COLUMN_NAME,
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

            streams = {}
            for (
                table_schema,
                table_name,
                column_name,
                data_type,
                character_maximum_length,
                numeric_position,
                numeric_scale,
                is_primary_key,
            ) in result_columns.fetchall():
                if table_name not in streams:
                    streams[table_name] = {}
                LOGGER.info(
                    f"Fetched {table_schema}.{table_name}.{column_name}"
                )

                stream = {
                    "tap_stream_id": table_name,
                    "stream": table_name,
                    "schema": {
                        "type": ["null", "object"],
                        "additionalProperties": False,
                        "properties": {
                            "TO-DO: id": {
                                "type": ["null", "string"],
                            },
                            "TO-DO: date_modified": {
                                "type": ["null", "string"],
                                "format": "date-time",
                            },
                        },
                    },
                    "metadata": [
                        {
                            "metadata": {
                                "inclusion": "available",
                                "table-key-properties": ["id"],
                                "selected": True,
                                "valid-replication-keys": ["date_modified"],
                                "schema-name": "users",
                            },
                            "breadcrumb": [],
                        },
                        {
                            "metadata": {"inclusion": "automatic"},
                            "breadcrumb": ["properties", "id"],
                        },
                        {
                            "metadata": {
                                "inclusion": "available",
                                "selected": True,
                            },
                            "breadcrumb": ["properties", "name"],
                        },
                        {
                            "metadata": {"inclusion": "automatic"},
                            "breadcrumb": ["properties", "date_modified"],
                        },
                    ],
                }

                streams[table_name] = stream

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

            output = {"streams": list(streams.values())}
            LOGGER.info(f"output = {output}")
            return output
