from pathlib import Path

import click
from dotenv import load_dotenv

from elysium_migration.migration.coordinator import ImportCoordinator
from elysium_migration.cmds.config import (
    envs_check,
    validation_dir_option,
    validate_option,
    inject_envs_file_option,
    script_dir_option,
    env_dir_option,
    config_path_option,
    output_path_option,
    log_level_option,
    log_path_option,
    write_cli_log_messages,
)


@click.group()
def apply():
    pass


@apply.command("import")
@validation_dir_option
@validate_option
@inject_envs_file_option
@script_dir_option
@env_dir_option
@config_path_option
@output_path_option
@log_level_option
@log_path_option
def import_cli(
    config_path,
    output_path,
    env_dir,
    script_dir,
    val_dir,
    inject_envs_from_env_file,
    validate,
    log_level,
    log_path
):

    write_cli_log_messages()
    envs_check(inject_envs_from_env_file=inject_envs_from_env_file, env_dir=env_dir)
    ImportCoordinator.import_data(
        config_file_path=Path(config_path),
        output_path=Path(output_path),
        inject_envs=inject_envs_from_env_file,
        validate=validate,
        val_dir=Path(val_dir),
        script_dir=script_dir,
        env_dir=env_dir,
    )
