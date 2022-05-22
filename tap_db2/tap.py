"""Db2 tap class."""

# from typing import List

from singer_sdk import SQLTap  # , SQLStream
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_db2.client import Db2Stream


class TapDb2(SQLTap):
    """Db2 tap class."""

    name = "tap-db2"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property(
            "auth_token",
            th.StringType,
            # required=True,
            description="The token to authenticate against the API service",
        ),
        th.Property(
            "project_ids",
            th.ArrayType(th.StringType),
            # required=True,
            description="Project IDs to replicate",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync",
        ),
        th.Property(
            "api_url",
            th.StringType,
            default="https://api.mysample.com",
            description="The url for the API service",
        ),
    ).to_dict()

    default_stream_class = Db2Stream
