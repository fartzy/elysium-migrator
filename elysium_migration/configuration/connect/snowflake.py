import os
from getpass import getpass

import snowflake.connector

from elysium_migration.configuration.connect import Connection
from elysium_migration.migration.config import DataSourceType


# TODO: use configs as parameter to constructor
class SnowflakeConnection(Connection):
    """
    Connection object for Snowflake
    """

    def __init__(self):
        super(SnowflakeConnection, self).__init__(DataSourceType.SNOWFLAKE)

    @property
    def ctx(self):
        return self._ctx

    @ctx.setter
    def ctx(self):
        self._ctx = _connect(self)

    def _connect(self):
        """
            return the connection obect
        """
        snowflake_pwd = getpass("Enter your password: ")

        self._ctx = snowflake.connector.connect(
            user=os.environ["SNOWFLAKE_USER"],
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            authenticator=os.environ["SNOWFLAKE_AUTHENTICATOR"],
            role=os.environ["SNOWFLAKE_ROLE"],
            warehouse=os.environ["SNOWFLAKE_ACCOUNT"],
            database=os.environ["SNOWFLAKE_DB"],
            schema=os.environ["SNOWFLAKE_SCHEMA"],
            password=snowflake_pwd,
        )
