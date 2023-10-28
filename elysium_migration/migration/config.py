import os
import subprocess
from pathlib import Path
from typing import Any, Dict

import yaml
from enum import Enum
from functools import partial

import elysium_migration
from elysium_migration import Logger


class ImmutableSingletonError(Exception):
    pass


class SingletonNotSetError(Exception):
    pass


# TODO:
# Refactor out Vertica to the other ones as well
# Refactor out the environment variables as well
# should put env variables in another class
class Config:
    """Singleton class to contain the configurations from the config file 
    
        Args:
            None 

        Attributes:
            __instance (__ConfigImmutableSingleton): real instance of configurations 
    """
    logger = None
    
    @classmethod
    def get_logger(cls):
        if cls.logger is None:
            cls.logger = Logger(log_name=__name__)
        return cls.logger
    
    class __ConfigImmutableSingleton:
        def __init__(self, name, objects: Dict[str, Any] = None):
            self.objects = objects
            self.name = name

        def contains(self, item: str):
            return item in self.objects

        def to_dict(self):
            return self.__dict__

    __instance = None

    @staticmethod
    def set_from_dict(dictionary) -> "__ConfigImmutableSingleton":
        if Config.__instance is None:
            Config.__instance = Config.__ConfigImmutableSingleton(**dictionary)
        else:
            raise ImmutableSingletonError(
                "Config is an immutable singleton, values should only be set once."
            )

    @staticmethod
    def read_yaml(path: Path) -> Dict[str, Any]:
        with path.open("r") as f:
            return yaml.load(f, Loader=yaml.SafeLoader)

    @staticmethod
    def set_from_yaml(path: Path) -> "__ConfigImmutableSingleton":
        Config.set_from_dict(Config.read_yaml(path))

    @staticmethod
    def get_project_dir() -> Path:
        return Path(elysium_migration.__path__[0]).parent

    @staticmethod
    def set_initial_envs(script_dir):
        output = Config.parse_envs(script_dir)

        # set the environment variables from .env
        # the delimiter || is for the script `getenv.sh` line
        # TODO: change this delimiter to NOT need to be synced with getenv.sh
        for s in output:
            logger.log.debug("Setting Environment variable: " + s.replace("||", "="))
            kv = s.split("||")
            os.environ[kv[0]] = kv[1]

    @staticmethod
    def parse_envs(script_dir):

        logger.log.debug(f"Executing getenv script in script dir: '{script_dir}'")
        # retrieve the environment variables from .env
        output = (
            subprocess.check_output(
                f". {script_dir}/getenv.sh", shell=True, stderr=subprocess.STDOUT,
            )
            .decode()
            # handle microsoft carriage return chars if they are there
            .replace(chr(13), "")
            .splitlines()
        )

        Config.get_logger().log.debug(f"Collected environment variables: '{output}'")
        return output

    @staticmethod
    def set_vsql_envs():
        # set environment variables needed for vsql
        # https://www.vertica.com/docs/9.2.x/HTML/Content/Authoring/ConnectingToVertica/vsql/vsqlEnvironmentVariables.htm

        os.environ[ConstantCatalog.VSQL_HOST] = os.environ["VERTICA_HOST"]
        os.environ[ConstantCatalog.VSQL_USER] = os.environ["VERTICA_USER"]
        os.environ[ConstantCatalog.VSQL_DATABASE] = os.environ["VERTICA_DB"]
        os.environ[ConstantCatalog.VSQL_PORT] = os.environ["VERTICA_PORT"]

        vsql_password = getpass(ConstantCatalog.VSQL_PWORD_PROMPT)
        os.environ[ConstantCatalog.VSQL_PASSWORD] = vsql_password

    @staticmethod
    def set_ybload_envs():

        os.environ[ConstantCatalog.YB_HOST] = os.environ["YBHOST"]
        os.environ[ConstantCatalog.YB_DATABASE] = os.environ["YBDATABASE"]
        os.environ[ConstantCatalog.YB_USER] = os.environ["YBUSER"]

        yellowbrick_pwd = getpass(ConstantCatalog.YB_PWORD_PROMPT)
        os.environ[ConstantCatalog.YB_PASSWORD] = yellowbrick_pwd

    @staticmethod
    def set_envs():
        """Sets environment variables for each DataSourceType

        Args:
            None

        Raises:
            None

        Returns:
            None

        """
        for platform in DataSourceType:
            platform()

    def __getattr__(self, name):
        # dont go to singleton for these funcs
        if self.__instance is None:
            raise SingletonNotSetError("The singleton for Config is not set.")
        return getattr(self.__instance, name)


class DataSourceType(Enum):
    """Enum to differentiate between systems 
    
        Args:
            None 

        Attributes:
            None
    """
    # SNOWFLAKE =
    VERTICA = partial(Config.set_vsql_envs)
    YELLOWBRICK = partial(Config.set_ybload_envs)

    def __call__(self, *args):
        self.value(*args)


config = Config()
