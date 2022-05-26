"""Db2 tap class."""

from typing import Dict, List

from singer_sdk import SQLTap
from singer_sdk.typing import (
    DateTimeType,
    PropertiesList,
    Property,
)

from singer_sdk.streams import Stream

from tap_db2.streams import Db2Stream


class Db2Tap(SQLTap):
    """Db2 tap class."""

    name = "tap-db2"
    default_stream_class = Db2Stream

    config_jsonschema = PropertiesList(
        Property(
            "start_date",
            DateTimeType,
            description="The earliest record date to sync",
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Initialize all available streams and return them as a list.

        Returns:
            List of discovered Stream objects.
        """
        result: List[Stream] = []
        for catalog_entry in self.catalog_dict["streams"]:
            result.append(self.default_stream_class(self, catalog_entry))

        return result

    @property
    def catalog_dict(self) -> dict:
        """Get catalog dictionary.

        Returns:
            The tap's catalog as a dict
        """
        if self._catalog_dict:
            return self._catalog_dict

        if self.input_catalog:
            return self.input_catalog.to_dict()

        connector = self.default_stream_class.connector_class(
            dict(self.config)
        )

        result: Dict[str, List[dict]] = {"streams": []}
        result["streams"].extend(connector.discover_catalog_entries())

        self._catalog_dict = result
        return self._catalog_dict


cli = Db2Tap.cli
