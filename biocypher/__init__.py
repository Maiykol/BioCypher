#!/usr/bin/env python

#
# Copyright 2021, Heidelberg University Clinic
#
# File author(s): Sebastian Lobentanzer
#                 ...
#
# Distributed under GPLv3 license, see the file `LICENSE`.
#
"""
Graph database standard for molecular biology
"""

__all__ = [
    '__version__',
    '__author__',
    'module_data',
    'config',
    'logfile',
    'log',
    'Driver',
    'Neo4jDriver',
    'PostgresDriver'
]

from ._config import config, module_data
from .db_modules.neo4j._driver import Neo4jDriver as Driver
from .db_modules.neo4j._driver import Neo4jDriver
from .db_modules.postgresql._driver import PostgresDriver
from ._logger import log, logfile
from ._metadata import __author__, __version__
