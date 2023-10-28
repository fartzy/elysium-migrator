import datetime
import logging
import os

import click

from elysium_migration import Logger
from elysium_migration.migration.utility import MigrationUtility

log_messages = []


def write_cli_log_messages():
    for message in log_messages:
        logger = Logger(log_name=__name__)
        logger.log.info(message)

        
def validate_truncate(ctx, param, value):
    exception_message = "User cancelled. The --truncate-tables option was chosen by the user but the user is not really 'bout that lyfe.\n"
    if (
        input(
            "\nAre you sure you want to truncate all the tables in the target database?\n\n\t\t'Y' or 'y' to continue: "
        ).lower()
        == "y"
    ):
        if (
            input(
                "\n\nAre you really serious? You want to truncate all the tables in the target database?\n\n\t\t'S' or 's' if you are super serious: "
            ).lower()
            == "s"
        ):
            log_messages.append(f"-------------- Truncating Tables!!! --------------")
            return value
        else:
            raise click.ClickException(exception_message)
    else:
        raise click.ClickException(exception_message)


def conflicting_sample_check(from_date, to_date, sample_size):
    inputs = [any([from_date, to_date]), sample_size]

    if all(inputs) is True:
        raise click.ClickException(
            "\n\tPredicate conflict: --sample-size can not be provided when either '--from-date' or '--to-date' options are provided also.\n"
        )


def envs_check(inject_envs_from_env_file, env_dir):
    inputs = [inject_envs_from_env_file, env_dir]

    if inject_envs_from_env_file and not env_dir:
        raise click.ClickException(
            "\n\tOption --inject-envs-from-env-file ( or just -i ) was not turned off, yet there was no --env-dir ( or just -ed ) path input by user. "
            + "Turn the flag off by just using the ( -ni  ) option if you don't want to use the .env file. Or just give the path of the .env file using the ( -ed ) option.\n"
        )


def verify_dates(from_date, to_date):
    date_format = "%Y-%m-%d %H:%M:%S"
    try:
        if from_date:
            from_date_obj = datetime.datetime.strptime(from_date, date_format)
            log_messages.append(
                f"-------------- --from-date is set to '{from_date_obj}' -------------- "
            )
        if to_date:
            to_date_obj = datetime.datetime.strptime(to_date, date_format)
            log_messages.append(
                f"-------------- --to-date is set to '{to_date_obj}' -------------- "
            )
    except ValueError:
        raise click.ClickException(
            "If '--from-date' and/or '--to-date' is provided, they NEED to be in ISO 8601 format - '%Y-&m-%d HH:MM:SS'"
        )


def env_dir_option(f):
    def env_dir_callback(ctx, param, value):
        if value:
            log_messages.append(
                f"-------------- Environment variables are loaded from file --------------"
            )
        return value

    return click.option(
        "--env-dir",
        "-ed",
        callback=env_dir_callback,
        type=click.Path(exists=True),
        required=False,
        help="This option is the path of the directory of the .env file.",
    )(f)


def inject_envs_file_option(f):
    def envs_callback(ctx, param, value):
        if value:
            log_messages.append(
                f"-------------- Environment variables are loaded from file --------------"
            )
        return value

    return click.option(
        "--inject-envs-from-env-file/--no-inject-envs-from-env-file",
        "-i/-ni",
        is_flag=True,
        callback=envs_callback,
        default=True,
        help="This option when set to True will allow to use the .env file to load all the environment variables.",
    )(f)


def script_dir_option(f):
    def script_dir_callback(ctx, param, value):
        if value:
            log_messages.append(
                f"-------------- Scripts path is '{value}' --------------"
            )
        return value

    return click.option(
        "--script-dir",
        "-sc",
        callback=script_dir_callback,
        type=click.Path(exists=True),
        default=MigrationUtility.get_root_dir() / "scripts",
        help="""
            This option is the path of the scripts directory with yb_checksum.sh and getenv.sh. The default works fine though. 
            The main reason to set this if you want to change the script wtihout rebuilding all the code.
            """,
    )(f)


def validate_option(f):
    def validate_callback(ctx, param, value):
        if value is True:
            log_messages.append(
                f"-------------- Validate is set to 'True' --------------"
            )
        else:
            log_messages.append(
                f"-------------- Validate is set to 'False' --------------"
            )
        return value

    return click.option(
        "--validate/--no-validate",
        "-v/-nv",
        is_flag=True,
        callback=validate_callback,
        default=True,
        help="""
            This option is to determine if we will do the validation or not. The import following an export won't work 
            unless that previous export had '-v' too.  So if you run an export with '-nv' and then run an import with 
            '-v' in the very next import, don't expect validation to work.""",
    )(f)


def config_path_option(f):
    def config_path_callback(ctx, param, value):
        log_messages.append(
            f"-------------- Confif file path is'{value}' --------------"
        )
        return value

    return click.option(
        "--config-path",
        "-c",
        callback=config_path_callback,
        type=click.Path(exists=True),
        required=True,
        help="This option is the path of the config file that will determine the database objects to migrate.",
    )(f)


def output_path_option(f):
    def output_path_callback(ctx, param, value):
        log_messages.append(
            f"-------------- Output directory is set to '{value}' --------------"
        )
        return value

    return click.option(
        "--output-path",
        "-o",
        callback=output_path_callback,
        type=click.Path(exists=True),
        required=True,
        help="This option is the path of the dir where the output of the migration will be stored.",
    )(f)


def validation_dir_option(f):
    def validation_dir_callback(ctx, param, value):
        log_messages.append(
            f"-------------- Validation directory set to '{value}'. --------------"
        )
        return value

    return click.option(
        "--val-dir",
        "-vd",
        callback=validation_dir_callback,
        type=click.Path(exists=True),
        required=True,
        help="This option is the path where the results of the yb_checksum validation will go.",
    )(f)


def sample_size_option(f):
    def sample_size_callback(ctx, param, value):
        if value > 0:
            log_messages.append(
                f"-------------- SAMPLE SIZE set to '{value}' --------------"
            )
        return value

    return click.option(
        "--sample-size",
        "-s",
        callback=sample_size_callback,
        type=click.INT,
        default=0,
        help="This option when set will determine the amount of records that will be retrieved per table. In some cases, more or less records may be retrieved.",
    )(f)


def log_level_option(f):
    def log_level_callback(ctx, param, value):
        os.environ["LOGLEVEL"] = value
        log_messages.append(
            f"-------------- Logging level '{value}' is set. --------------"
        )
        return value

    return click.option(
        "--log-level",
        "-ll",
        callback=log_level_callback,
        type=click.STRING,
        default="INFO",
        help="""
            This option will determine the level of logging for this execution of the application. 
            The valid levels are:
                CRITICAL
                ERROR
                WARNING
                INFO
                DEBUG
                NOTSET""",
    )(f)
 
    
#This is set to eager to force evaluation first
def log_path_option(f):
    def log_path_callback(ctx, param, value):
        if value:
            logger = Logger(log_name=__name__, log_path=value)
            log_messages.append(
                f"-------------- Logging path set to '{value}'. --------------"
            )
        return value

    return click.option(
        "--log-path",
        "-lp",
        callback=log_path_callback,
        type=click.STRING,
        is_eager=True,
        default="app.log",
        help="""This option is the whole absolute path of log file.""",
    )(f)
