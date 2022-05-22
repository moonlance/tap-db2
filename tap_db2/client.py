"""SQL client handling.

This includes Db2Stream and Db2Connector.
"""

import sqlalchemy

from singer_sdk import SQLConnector, SQLStream
from typing import Any, Dict, Iterable, Optional


class Db2Connector(SQLConnector):
    """Connects to the Db2 SQL source."""

    def get_sqlalchemy_url(cls, config: dict) -> str:
        """Concatenate a SQLAlchemy URL for use in connecting to the source."""

        return "ibm_db_sa+pyodbc://{}:{}@{}:{}/{}".format(
            config["username"],
            config["password"],
            config["hostname"],
            config["port"],
            config["database"],
        )

    @staticmethod
    def to_jsonschema_type(sql_type: sqlalchemy.types.TypeEngine) -> dict:
        """Returns a JSON Schema equivalent for the given SQL type.

        Developers may optionally add custom logic before calling the default
        implementation inherited from the base class.
        """
        # Optionally, add custom logic before calling the super().
        # You may delete this method if overrides are not needed.
        return super().to_jsonschema_type(sql_type)

    @staticmethod
    def to_sql_type(jsonschema_type: dict) -> sqlalchemy.types.TypeEngine:
        """Returns a JSON Schema equivalent for the given SQL type.

        Developers may optionally add custom logic before calling the default
        implementation inherited from the base class.
        """
        # Optionally, add custom logic before calling the super().
        # You may delete this method if overrides are not needed.
        return super().to_sql_type(jsonschema_type)


class Db2Stream(SQLStream):
    """Stream class for Db2 streams."""

    connector_class = Db2Connector

    def get_records(
        self, partition: Optional[dict]
    ) -> Iterable[Dict[str, Any]]:
        """Return a generator of row-type dictionary objects.

        Developers may optionally add custom logic before calling the default
        implementation inherited from the base class.

        Args:
            partition:
                If provided, will read specifically from this data slice.

        Yields:
            One dict per record.
        """
        # Optionally, add custom logic instead of calling the super().
        # This is helpful if the source database provides batch-optimized
        # record retrieval.
        # If no overrides or optimizations are needed, you may delete this
        # method.
        yield from super().get_records(partition)
