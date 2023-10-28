from pathlib import Path

import pytest

from tests.fixtures import (
    sample_config_file,
    sample_input_config_file,
    set_yb_envs,
    setenvs,
)
from elysium_migration import logger
from elysium_migration.migration.config import config
from elysium_migration.migration.execute import Execution
from elysium_migration.migration.importer import Importer


@pytest.mark.parametrize(
    "input_path",
    [
        "/logs/work/martz1/dev/elysium-migration-data/from-vertica/Elysium.FINGAM_Enrichments_Rejected/1.csv"
    ],
)
def test_import_table_data_tab_delimiter(sample_config_file, set_yb_envs, input_path):

    test_table = "Elysium.FINGAM_Enrichments_Rejected"

    out = Execution.ybload(
        table=test_table,
        input_path=input_path,
        extras=test_configs,
        field_delimiter="\t",
    )

    num_lines = 0
    assert False


@pytest.mark.parametrize(
    "input_path",
    [Path("/logs/work/martz1/dev/elysium-migration-data/to-yellowbrick-all")],
)
def test_import_all_tables(sample_input_config_file, set_yb_envs, input_path):

    config.set_from_yaml(sample_input_config_file)
    import_objects = config.objects

    importer = Importer(import_objects, input_path)

    importer.import_tables()

    assert False


@pytest.mark.parametrize(
    "input_path", [Path("/logs/work/martz1/dev/elysium-migration-data/from-vertica")],
)
def test_import_all_tables_from_output(sample_config_file, set_yb_envs, input_path):
    extras = """--on-unescaped-embedded-quote PRESERVE --logfile /home/martz1/dev/elysium-migration/yblog.log --logfile-log-level TRACE --format csv"""
    config.set_from_yaml(sample_config_file)
    import_objects = config.objects

    importer = Importer(import_objects, input_path)

    importer.import_tables()

    # testing if there are no errors basically while loading
    assert True


@pytest.mark.parametrize(
    "input_file",
    [
        "/logs/work/martz1/dev/elysium-migration-data/from-vertica/Elysium.FINGAM_Orders_Rejected/0.csv"
    ],
)
def test_pre_process(sample_config_file, input_file, capsys):
    in_file = open(input_file, "r")
    lines = in_file.readlines()
    in_file.close()

    new_lines = []
    for line in lines:
        new_line = line.replace("\t{", "\t'{").replace("}\t", "}'\t")
        with capsys.disabled():
            print(new_line)
            print(line)
        new_lines.append(new_line)

    with open("new_file.csv", "w+") as f:
        for new_line in new_lines:
            f.write(new_line)

    assert False


@pytest.mark.parametrize(
    "input_path", [Path("/logs/work/martz1/dev/elysium-migration-data/from-vertica")],
)
def test_import_all_tables_from_output_no_configs(set_yb_envs, input_path):

    import_objects = config.objects

    importer = Importer(import_objects, input_path)

    importer.import_tables()

    # testing if there are no errors basically while loading
    assert True
