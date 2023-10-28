import datetime
import os
import typing as t
from pathlib import Path

import click
from elysium_migration import logger
from elysium_migration.configuration.housekeep import HouseKeeper
from elysium_migration.migration.execute import ScriptCatalog
from elysium_migration.migration.utility import MigrationUtility
from elysium_migration.cmds.config import (
    envs_check,
    verify_dates,
    validate_truncate,
    inject_envs_file_option,
    script_dir_option,
    env_dir_option,
    log_level_option,
    write_cli_log_messages,
    set_logging,
)


@click.group()
def housekeep():
    pass


@housekeep.command("housekeep")
@click.option(
    "--truncate-tables/--no-truncate-tables",
    "-t/-nt",
    is_flag=True,
    callback=validate_truncate,
    default=False,
    help="This is the option to truncate all the data in the target database.  Dont do this unless you really want to.",
)
@click.option(
    "--log-cleanup",
    "-lc",
    type=click.Path(exists=True),
    is_flag=True,
    default=False,
    help="This option will delete the log in the default location. Not implemented yet.",
)
@inject_envs_file_option
@script_dir_option
@env_dir_option
@log_level_option
def housekeep_cli(
    truncate_tables,
    log_cleanup,
    inject_envs_from_env_file,
    script_dir,
    env_dir,
    log_level,
):
    set_logging(log_level=log_level)
    write_cli_log_messages()
    envs_check(inject_envs_from_env_file=inject_envs_from_env_file, env_dir=env_dir)
    HouseKeeper.work(
        truncate_tables=truncate_tables,
        log_cleanup=log_cleanup,
        inject_envs_from_env_file=inject_envs_from_env_file,
        script_dir=script_dir,
        env_dir=env_dir,
    )
