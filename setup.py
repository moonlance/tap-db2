#!/usr/bin/env python

from setuptools import setup

setup(
    name="tap-db2",
    version="1.0.0",
    description="Tap for extracting data from an IBM DB2 database",
    author="Tom Sloman",
    url="TO-DO",
    classifiers=[
        # "License :: OSI Approved :: GNU Affero General Public License v3",
        "Programming Language :: Python :: 3 :: Only",
    ],
    py_modules=["tap_db2"],
    install_requires=[
        "attrs==21.4.0",
        "pendulum==1.2.0",
        "markupsafe<2.1.0",
        "singer-python==5.9.0",
        "sqlalchemy<2.0.0",
        "pyodbc==4.0.26",
        "ibm-db==3.1.1",
        "ibm-db-sa==0.3.7",
        "backoff==1.8.0",
        "jinja2==2.11.3",
    ],
    entry_points="""
          [console_scripts]
          tap-db2=tap_db2:main
      """,
    packages=["tap_db2", "tap_db2.sync_strategies"],
)
