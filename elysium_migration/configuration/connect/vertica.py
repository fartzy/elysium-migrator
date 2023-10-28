import os
from getpass import getpass

import pyodbc

from elysium_migration.configuration.connect import Connection
from elysium_migration.migration.config import DataSourceType


class VerticaConnection(Connection):
    """
    Connection object for Vertica
    """

    def __init__(self):
        super(VerticaConnection, self).__init__(DataSourceType.VERTICA)
        self.ctx = self._connect()

    def _connect(self):
        """
            return the connection obect
        """
        vertica_pwd = getpass("Enter your password: ")

        vertica_conn = pyodbc.connect(
            DRIVER="Vertica",
            SERVERNAME=os.environ["VERTICA_HOST"],
            DATABASE=os.environ["VERTICA_DB"],
            PORT=os.environ["VERTICA_PORT"],
            UID=os.environ["VERTICA_USER"],
            PWD=vertica_pwd,
        )

        return vertica_conn
