import typing as t
from pathlib import Path

import click
from dotenv import loadenv

from elysium_migration.migration.coordinator import ExportCoordinator
from elysium_migration.cmds.config import (
    conflicting_sample_check,
    envs_check,
    verify_dates,
    validate_option,
    inject_envs_file_option,
    script_dir_option,
    env_dir_option,
    config_path_option,
    output_path_option,
    sample_size_option,
    log_level_option,
    log_path_option,
    write_cli_log_messages,
)


@click.group()
def export():
    pass


@export.command("export")
@click.option(
    "--from-date",
    "-fd",
    type=click.STRING,
    required=False,
    help="This option of type date is the min date to put in the where clause. I.e., retrieve records after this date.",
)
@click.option(
    "--to-date",
    "-td",
    type=click.STRING,
    required=False,
    help="This option of type date is the max date to put in the where clause. I.e., retrieve all records before this date.",
)
@validate_option
@inject_envs_file_option
@script_dir_option
@env_dir_option
@config_path_option
@output_path_option
@sample_size_option
@log_level_option
@log_path_option
def export_cli(
    config_path,
    output_path,
    inject_envs_from_env_file,
    sample_size,
    validate,
    script_dir,
    from_date,
    to_date,
    env_dir,
    log_level,
    log_path
):
    if inject_envs_from_env_file:
        load_dotenv(env_dir)
        
    verify_dates(from_date, to_date)
    write_cli_log_messages()
    conflicting_sample_check(from_date, to_date, sample_size)
    envs_check(inject_envs_from_env_file=inject_envs_from_env_file, env_dir=env_dir)

    ExportCoordinator.export(
        config_file_path=Path(config_path),
        output_path=Path(output_path),
        inject_envs=inject_envs_from_env_file,
        sample_size=sample_size,
        validate=validate,
        script_dir=script_dir,
        from_date=from_date,
        to_date=to_date,
        env_dir=env_dir,
    )
