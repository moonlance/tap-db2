from singer_sdk import Tap
from singer_sdk import typing

from tap_db2.stream import StreamDb2


class TapDb2(Tap):
    """
    Db2 tap class.
    """

    name = "tap-db2"

    config_jsonschema = typing.PropertiesList(
        typing.Property("username", typing.StringType, required=True),
        typing.Property("password", typing.StringType, required=True),
        typing.Property("hostname", typing.StringType, required=True),
        typing.Property("port", typing.StringType, required=True),
        typing.Property("database", typing.StringType, required=True),
    ).to_dict()

    @property
    def catalog_dict(self) -> dict:
        if self.input_catalog:
            return self.input_catalog

        return StreamDb2.run_discovery(self.config)

    def discover_streams(self) -> typing.List[StreamDb2]:
        """
        Return a list of discovered streams.
        """
        result: typing.List[StreamDb2] = []

        for catalog_entry in self.catalog_dict["streams"]:
            result.append(StreamDb2(self, catalog_entry))

        return result
