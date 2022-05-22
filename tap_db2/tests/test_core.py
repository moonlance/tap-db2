"""Tests standard tap features using the built-in SDK tests library."""

import datetime

from singer_sdk.testing import get_standard_tap_tests

from tap_db2.tap import TapDb2

SAMPLE_CONFIG = {
    "username": "db2inst1",
    "password": "ADMIN",
    "hostname": "localhost",
    "port": "50000",
    "database": "TESTDB",
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime(
        "%Y-%m-%d"
    )
    # TODO: Initialize minimal tap config
}


# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests():
    """Run standard tap tests from the SDK."""
    tests = get_standard_tap_tests(TapDb2, config=SAMPLE_CONFIG)
    for test in tests:
        test()


# TODO: Create additional tests as appropriate for your tap.
