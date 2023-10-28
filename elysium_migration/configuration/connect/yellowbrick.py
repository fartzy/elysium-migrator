import os
from getpass import getpass

from elysium_migration.configuration.connect import Connection
from elysium_migration.migration.config import DataSourceType


# TODO: use configs as parameter to constructor
class YellowBrickConnection(Connection):
    """
    Connection object for YellowBrick
    """

    def __init__(self):
        super(YellowBrickConnection, self).__init__(DataSourceType.YELLOWBRICK)

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
        yellowbrick_pwd = getpass("Enter your password: ")

        os.environ[YBLOAD_PASSWORD] = yellowbrick_pwd
