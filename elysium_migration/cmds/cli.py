import os
from gettext import gettext as _
from pathlib import Path
import typing as t

import click
import click_log
import datetime

from elysium_migration import CONTEXT_SETTINGS, logger
from elysium_migration.configuration.housekeep import HouseKeeper
from elysium_migration.migration.utility import MigrationUtility
from elysium_migration.migration.execute import ScriptCatalog
from elysium_migration.migration.coordinator import (
    ExportCoordinator,
    ImportCoordinator,
)

from elysium_migration.cmds.housekeeper_cli import housekeep
from elysium_migration.cmds.export_cli import export
from elysium_migration.cmds.import_cli import apply


@click.group()
def cli():
    pass


cli = click.CommandCollection(sources=[housekeep, export, apply])

if __name__ == "__main__":
    cli()
