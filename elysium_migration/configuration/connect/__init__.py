import os
from getpass import getpass

import snowflake.connector
from enum import Enum


class Connection:
    """
    This class is an interface for handling connections to data sources that involve long lived connections 
    (i.e. pydobc, SQLAlchemy, etc)
    """

    ctx = None

    def __init__(self, platform):
        self.platform = platform

    def _connect(self):
        pass

    @property
    def ctx(self):
        return self._ctx

    @ctx.setter
    def ctx(self):
        self._ctx = None

    def __enter__(self):
        return self.ctx

    def __exit__(self, type, value, traceback):
        self.ctx.close()
