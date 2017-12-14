#!/usr/bin/env python3
from setuptools import setup

setup(
    name="tap-db2",
    version="0.1.0",
    description="Singer.io tap for extracting data from DB2",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    install_requires=[
        "singer-python>=3.2.0",
        "pyodbc",
    ],
    entry_points="""
    [console_scripts]
    tap-db2=tap_db2:main
    """,
    packages=["tap_db2", "tap_db2.discovery"],
    include_package_data=True,
)
