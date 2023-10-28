import json
import os
import subprocess
from getpass import getpass
from pathlib import Path

import pytest

from tests import configuration
from elysium_migration.configuration.connect.vertica import VerticaConnection
from elysium_migration.configuration.constants import ConstantCatalog
from elysium_migration.migration.execute import StatementCatalog


@pytest.fixture
def root_project_dir():
    path = Path(configuration.__path__[0]).parent.parent
    return path


@pytest.fixture
def sample_config_file():
    path = Path(configuration.__path__[0]) / "vertica_test.yaml"
    return path


@pytest.fixture
def sample_input_config_file():
    path = Path(configuration.__path__[0]) / "yellowbrick_test.yaml"
    return path


@pytest.fixture
def sample_snowflake_ddl():
    path = (
        Path(configuration.__path__[0]).parent
        / "sql"
        / "snowflake"
        / "schema"
        / "create-table-VH_TEST.sql"
    )
    return path


@pytest.fixture
def setenvs(root_project_dir):

    # retrieve the environment variables from .env
    output = (
        subprocess.check_output(
            StatementCatalog.getenvs(root_project_dir),
            shell=True,
            stderr=subprocess.STDOUT,
        )
        .decode()
        # handle microsoft carriage return chars
        .replace(chr(13), "")
        .splitlines()
    )

    # set the environment variables from .env
    for s in output:
        kv = s.split("||")
        os.environ[kv[0]] = kv[1]


@pytest.fixture
def set_vsql_envs(setenvs):
    setenvs
    # set environment vairbales needed for vsql
    # https://www.vertica.com/docs/9.2.x/HTML/Content/Authoring/ConnectingToVertica/vsql/vsqlEnvironmentVariables.htm
    os.environ[ConstantCatalog.VSQL_HOST] = os.environ["VERTICA_HOST"]
    os.environ[ConstantCatalog.VSQL_USER] = os.environ["VERTICA_USER"]
    os.environ[ConstantCatalog.VSQL_DATABASE] = os.environ["VERTICA_DB"]
    os.environ[ConstantCatalog.VSQL_PORT] = os.environ["VERTICA_PORT"]

    vsql_password = getpass(ConstantCatalog.VSQL_PWORD_PROMPT)
    os.environ[ConstantCatalog.VSQL_PASSWORD] = vsql_password


@pytest.fixture
def set_yb_envs(setenvs):
    setenvs
    os.environ[ConstantCatalog.YB_HOST] = os.environ["YBHOST"]
    os.environ[ConstantCatalog.YB_DATABASE] = os.environ["YBDATABASE"]
    os.environ[ConstantCatalog.YB_USER] = os.environ["YBUSER"]

    yellowbrick_pwd = getpass(ConstantCatalog.YB_PWORD_PROMPT)
    os.environ[ConstantCatalog.YB_PASSWORD] = yellowbrick_pwd


@pytest.fixture
def vertica_conn(setenvs):
    setenvs
    cursor = VerticaConnection()
    return cursor
