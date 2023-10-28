import glob
import hashlib
import os
import shutil
from datetime import date, datetime, timedelta
from operator import itemgetter
from pathlib import Path

import pytest
import yaml
from deepdiff import DeepDiff

from tests.exporttest import test_full_export_with_where_clause
from tests.fixtures import (
    root_project_dir,
    sample_config_file,
    sample_input_config_file,
    set_vsql_envs,
    set_yb_envs,
    setenvs,
)
from tests.importtest import test_import_all_tables_from_output_no_configs
from elysium_migration import Logger
from elysium_migration.configuration import Platform
from elysium_migration.configuration.constants import ConstantCatalog
from elysium_migration.migration.config import ImmutableSingletonError, config
from elysium_migration.migration.execute import Execution, StatementCatalog
from elysium_migration.migration.exporter import Exporter
from elysium_migration.migration.utility import MigrationUtility

logger = Logger(log_name=__name__, log_path=f"{datetime.strftime(date.today(), '%Y%m%s')}_{__name__}.log")

def test_create_validation_results(
    sample_config_file, set_vsql_envs, capsys, root_project_dir, set_yb_envs
):
    val_results_dir = root_project_dir / "tests" / "sql" / "validation" / "results"
    if val_results_dir.exists():
        shutil.rmtree(val_results_dir)
    os.makedirs(val_results_dir)

    val_queries_dir = root_project_dir / "tests" / "sql" / "validation" / "queries"
    if val_queries_dir.exists():
        shutil.rmtree(val_queries_dir)
    os.makedirs(val_queries_dir)

    results = []
    val_script_path = root_project_dir / "tests" / "scripts" / "yb_exec_checksum.sh"
    with open(val_path, "r") as f:
        lines = f.readlines()
        for command in lines:
            logger.log.debug(
                f"Executing validation command {command} from {val_script_path}..."
            )
            Execution._execute(command)

    with open(val_results_dir / "results.log", "w") as f:
        for fi in glob.glob(str(val_queries_dir) + "/*.sql"):
            logger.log.debug(f"Using validation sql {fi} ass input to vsql...")
            f.write(Execution._execute(f"vsql -A -t -x < {fi}").decode() + "\n")


@pytest.mark.parametrize(
    "output_path, validation_path",
    [
        (
            "/logs/work/martz1/dev/elysium-migration-data/from-vertica/",
            "/logs/work/martz1/dev/elysium-migration-data/yellowbrick-validation",
        )
    ],
)
def test_full_test(
    sample_config_file,
    set_vsql_envs,
    capsys,
    root_project_dir,
    set_yb_envs,
    output_path,
    validation_path,
):
    Execution._execute(
        f"ybsql < {root_project_dir}/tests/sql/yellowbrick/truncate-table-yb.sql"
    )
    test_full_export_with_where_clause(
        output_path=output_path,
        sample_config_file=sample_config_file,
        set_vsql_envs=set_vsql_envs,
        capsys=capsys,
        root_project_dir=root_project_dir,
    )
    test_import_all_tables_from_output_no_configs(
        set_yb_envs=set_yb_envs, input_path=Path(output_path),
    )
    test_validation_every_field(
        sample_config_file=sample_config_file,
        set_vsql_envs=set_vsql_envs,
        capsys=capsys,
        root_project_dir=root_project_dir,
        set_yb_envs=set_yb_envs,
        output_path=output_path,
        validation_path=validation_path,
        validation_rows_per_table=100,
    )
    test_create_validation_results(
        sample_config_file=sample_config_file,
        set_vsql_envs=set_vsql_envs,
        capsys=capsys,
        root_project_dir=root_project_dir,
        set_yb_envs=set_yb_envs,
    )


@pytest.mark.parametrize(
    "validation_path, output_path",
    [
        (
            "/logs/work/martz1/dev/elysium-migration-data/yellowbrick-validation",
            "/logs/work/martz1/dev/elysium-migration-data/from-vertica",
        )
    ],
)
def test_validation_from_file_hash(
    sample_config_file,
    set_vsql_envs,
    capsys,
    root_project_dir,
    set_yb_envs,
    output_path,
    validation_path,
):
    try:
        config.set_from_yaml(sample_config_file)
    except ImmutableSingletonError:
        print("config already set")
    export_objects = config.objects
    exporter = Exporter(export_objects, output_path)
    tables = exporter.table_partition_col_map.items()

    results = []
    for table, v in tables:
        val_path = f"{validation_path}/{table}"
        if not Path(val_path).exists():
            os.makedirs(val_path)

        order_by = "" if v is None else f" ORDER BY {v} DESC "

        ybsql = f"""ybsql -c "SELECT * FROM {table} {order_by} " -o {val_path}/0.csv -F '\t' -A -t"""
        Execution._execute(ybsql)
        lines = []
        with open(f"{val_path}/0.csv", "r") as f:
            lines = f.readlines()

        new_lines = []
        with open(f"{val_path}/0.csv", "w") as f:
            cols_list = list(
                map(lambda col: col.upper(), Execution.get_table_column_names(table))
            )
            f.write("\t".join(cols_list) + "\n")
            for line in lines:
                f.write(line)

        vertica_lines = []
        with open(f"{output_path}/{table}/0.csv", "r") as f:
            for line in f.readlines():
                vertica_lines.append(line.replace("\x00", ""))

        vertica_lines = list(filter(lambda s: len(s.strip()) > 0, vertica_lines[1:]))
        with open(c, "w") as f:
            cols_list = list(
                map(lambda col: col.upper(), Execution.get_table_column_names(table))
            )
            f.write("\t".join(cols_list) + "\n")
            for line in vertica_lines:
                f.write(line)

        yellowbrook_hash = hashlib.sha512(
            open(f"{val_path}/0.csv", "rb").read()
        ).hexdigest()

        vertica_hash = hashlib.sha512(
            open(f"{output_path}/{table}/1.csv", "rb").read()
        ).hexdigest()

        results.append(yellowbrook_hash == vertica_hash)

        os.remove(f"{output_path}/{table}/1.csv")

    assert all(results)


@pytest.mark.parametrize(
    "validation_path, output_path, validation_rows_per_table, predicate,tables,excluded_tables",
    [
        (
            "/logs/work/martz1/dev/elysium-migration-data/temp/yellowbrick-validation",
            "/logs/work/martz1/dev/elysium-migration-data/temp/from-vertica",
            1,
            " AND id >= '8263344003350' and id < '8263476000000'",
            {"Elysium.FINGAM_Transactions": "ID"},
            [],
        )
    ],
)
def test_validation_every_field(
    sample_config_file,
    set_vsql_envs,
    capsys,
    root_project_dir,
    set_yb_envs,
    output_path,
    validation_path,
    validation_rows_per_table,
    predicate,
    tables,
    excluded_tables,
):
    try:
        config.set_from_yaml(sample_config_file)
    except ImmutableSingletonError:
        print("config already set")
    export_objects = config.objects
    exporter = Exporter(export_objects, output_path, str(root_project_dir / "scripts"))

    results = []
    for table, v in tables.items():
        if table not in excluded_tables:
            val_path = f"{validation_path}/{table}"
            out_path = f"{output_path}/{table}"
            MigrationUtility.clear_dir(val_path)
            MigrationUtility.clear_dir(out_path)

            def handle_reserved_words(c, system="yellowbrick"):
                if system == "yellowbrick":
                    c = c.lower()
                if "uuid" == c.strip():
                    c = '\\"UUID\\"'
                return c

            vertica_col_predicate = ""

            raw_cols_list = Execution.get_table_column_names(
                table, platform=Platform.VERTICA
            )

            cols_list = [handle_reserved_words(c) for c in raw_cols_list]
            cols_expr = ",".join(cols_list)

            logger.log.debug(f"YB Column names for {table} are " + str(cols_list))

            # need to lowercase because yb is all lowercase and its case sensitive
            yellowbrick_table = table.lower()
            order_by = "" if v is None else f" ORDER BY {v.lower()} DESC "
            query = f""" SELECT {cols_expr} FROM {yellowbrick_table} WHERE 1=1 {predicate.lower()} {order_by} ;"""
            vquery = f""" SELECT {cols_expr} FROM {table} WHERE 1=1 {predicate} {order_by} ;"""
            Execution.ybsql(
                query=query,
                output_path=f"{val_path}/0.csv",
                field_delimiter="\t",
                extra_output_args=" -t ",
            )

            Execution.vsql(
                vquery,
                output_path=f"{out_path}/0.csv",
                field_delimiter=r"\t",
                extra_output_args=" -t ",
            )

            yb_lines = []
            with open(f"{val_path}/0.csv", "r") as f:
                for line in f.readlines():
                    yb_lines.append(line)

            vertica_lines = []
            with open(f"{out_path}/0.csv", "r") as f:
                for line in f.readlines():
                    vertica_lines.append(line)

            vertica_pandas_lines = [s.strip("\n").split("\t") for s in vertica_lines]
            yb_pandas_lines = [s.strip("\n").split("\t") for s in yb_lines]

            logger.log.debug(
                f"""
                Length of vertica_lines: {str(len(vertica_lines))} and length of yb_pandas_lines: {str(len(yb_lines))}
                """
            )

            d = DeepDiff(vertica_lines, yb_lines, ignore_order=True)

    assert len(d) == 0


@pytest.mark.parametrize(
    "validation_path, output_path, validation_rows_per_table, tables",
    [
        (
            "/logs/work/martz1/dev/elysium-migration-data/temp/yellowbrick-validation",
            "/logs/work/martz1/dev/elysium-migration-data/temp/from-vertica",
            1,
            {"Elysium.FINSECMM_Executions_T": "ID"},
        )
    ],
)
def test_validation_every_field_visual(
    sample_config_file,
    set_vsql_envs,
    capsys,
    root_project_dir,
    set_yb_envs,
    output_path,
    validation_path,
    validation_rows_per_table,
    tables,
):

    results = []
    excluded_tables = []
    for table, v in tables.items():
        if table not in excluded_tables:
            val_path = f"{validation_path}/{table}/0.csv"
            out_path = f"{output_path}/{table}/0.csv"
            min_val = Execution.vsql_get_sample_filter_val(
                table=table, part_col=v, sample_size=5000
            )
            max_val = Execution.vsql_get_max_col_val(table=table, column=v)
            predicate = ConstantCatalog.YB_CHECK_SUM_PREDICATE(
                table=table, part_col=v, min_val=min_val, max_val=max_val
            )

            cols = Execution.get_table_column_names(schema_and_table=table)
            cols_expr = ",".join(cols)

            v_query = StatementCatalog.select_from_table(
                schema_and_table=table,
                predicate=predicate,
                col_order_by_desc=v,
                cols_expr=cols_expr,
            )
            yb_query = StatementCatalog.select_from_table(
                schema_and_table=table,
                predicate=predicate,
                col_order_by_desc=v,
                cols_expr=cols_expr.lower().replace(",uuid,", ',\\"UUID\\",'),
            )
            Execution.vsql(
                query=v_query,
                output_path=out_path,
                field_delimiter=r"\t",
                extra_output_args=" -t ",
            )

            Execution.ybsql(
                query=yb_query,
                output_path=val_path,
                field_delimiter="\t",
                extra_output_args=" -t ",
            )

            yb_lines = []
            with open(val_path, "r") as f:
                for line in f.readlines():
                    yb_lines.append(line)

            vertica_lines = []
            with open(out_path, "r") as f:
                for line in f.readlines():
                    vertica_lines.append(line.replace("\x00", ""))
                    # vertica_lines.append(line)

            vertica_lines = [s.strip("\n").split("\t") for s in vertica_lines]
            yb_lines = [s.strip("\n").split("\t") for s in yb_lines]

            logger.log.debug(
                f"""
                Length of vertica_lines: {str(len(vertica_lines))} and length of yb_pandas_lines: {str(len(yb_lines))}
                """
            )
            if len(vertica_lines) == 0 and len(yb_lines) == 0:
                # no records
                continue

            try:
                vertica_lines.sort(key=itemgetter(*range(len(cols))))
                yb_lines.sort(key=itemgetter(*range(len(cols))))
            except IndexError:
                logger.log.error(
                    f"""
                    IndexError: First line of yb_lines: {str(yb_lines[0])} 
                                First line of vertica_lines : {str(vertica_lines[0])}
                """
                )
                raise
            logger.log.debug(
                f"Width of vertica dataset: {str(len(vertica_lines[0]))} -- Width of yellowbrick dataset: {str(len(yb_lines[0]))}"
            )

            # if validation_rows_per_table is set to less than 1, do validation for the whole data set
            if validation_rows_per_table < 1:
                validation_rows_per_table = len(vertica_lines)

            for ind, tuple_of_lines in enumerate(
                list(zip(vertica_lines, yb_lines))[:validation_rows_per_table]
            ):
                vline, yline = tuple_of_lines

                logger.log.debug(
                    f"""
                    Line from vertica dataset: {str(vline)}
                    Line from yellowbrick dataset: {str(yline)}
                    """
                )
                cols_len = len(vline)
                table_row_compare_results = []
                logger.log.info(f"Starting {table} field comparison: row {ind}")

                for i in range(cols_len):
                    col = cols[i]
                    e = vline[i] == yline[i]
                    table_row_compare_results.append(e)
                    logger.log.debug(
                        f"""
                        Row #{(str(ind))}, field #{str(i)} in {table}
                        vertica {col} and yellowbrick {col} are equal = {str(e).upper()}
                        vertica {col} = {str(vline[i])}
                        yellowb {col} = {str(yline[i])}             
                    """
                    )

                row_compare = all(table_row_compare_results)
                logger.log.info(f"{table} row {ind} are identical = {str(row_compare)}")
                results.append(row_compare)
            results.append(len(vertica_lines) == len(yb_lines))
    assert all(results)


@pytest.mark.parametrize(
    "file_path", ["/home/martz1/dev/vertica_elysium_cigam_transactions.csv"],
)
def test_delete_nulls_from_file(file_path):
    Elysium.FINGAM_Busts_T
    vertica_lines = []
    data = ""
    with open(file_path, "r") as f:
        data = f.readlines()

    data[0] = data[0].upper().replace("INT", "BIGINT")

    with open(file_path, "w") as f:
        for line in data:
            f.write(line.replace("\x00", ""))


@pytest.mark.parametrize(
    "output_path", ["/home/martz1/dev/vertica_elysium_cigam_transactions.csv"],
)
def test_compare_columns(
    set_vsql_envs, capsys, root_project_dir, set_yb_envs, output_path
):

    tables = [
        "Elysium.FINGAM_Transactions",
        "Elysium.FINGAM_Orders",
        "Elysium.FINGAM_Executions_T",
        "Elysium.FINGAM_Enrichments",
        "Elysium.FINGAM_Enrichments_Rejected",
        "Elysium.FINGAM_ComplexTrades",
        "Elysium.FINGAM_Orders_Rejected",
        "Elysium.FINGAM_Busts_T",
        "Elysium.FINSECMM_Orders",
        "Elysium.FINSECMM_Transactions",
        "Elysium.FINSECMM_Executions_T",
        "Elysium.FINSECMM_Busts_T",
        "Elysium.FINSECMM_Quotes",
        "Elysium.FINSECMM_Enrichments",
        "Elysium.FINSECMM_Enrichments_Rejected",
        "Elysium.FINSECMM_ComplexTrades",
        "Elysium.TKey_Mapping_T",
        "Elysium.TKey_Updates",
        "Elysium.BookmarkStore",
        "Elysium.Sweeper_Bookmark",
        "Elysium.TKEY_LATEST_MAPPINGS_T",
        "Elysium.TKey_Cancel_T",
        "Elysium.TKey_Cancel_All_T",
        "Compliance.DeskLevelUser",
        "Compliance.DeskLevelUserDB",
        "Compliance.OverTheEdgeUser",
        "Compliance.OverTheEdgeUserDB",
        "sandbox.TKEY_LATEST_MAPPINGS_T",
    ]

    table = "Elysium.FINGAM_Orders"

    with capsys.disabled():
        for table in tables:
            ybrick = Execution.get_table_column_names(
                schema_and_table=table, platform=Platform.YELLOWBRICK
            )
            vertic = Execution.get_table_column_names(schema_and_table=table)
            ybrick_cols = [col.lower() for col in ybrick]
            vertic_cols = [col.lower() for col in vertic]
            ddiff = DeepDiff(
                ybrick_cols, vertic_cols, report_repetition=True, ignore_order=True
            )
            ddiffopp = DeepDiff(
                vertic_cols, ybrick_cols, report_repetition=True, ignore_order=True
            )
            logger.log.debug(f"\nTable {table} comparison: \n\n{ddiff}")
            print(f"\nTable {table} comparison: \n\n{ddiff}")
            print(f"\nTable {table} comparison: \n\n{ddiffopp}")

    assert [col.upper() for col in ybrick_cols] == [col.upper() for col in vertic_cols]


@pytest.mark.parametrize(
    "output_path", ["/home/martz1/dev/vertica_elysium_cigam_transactions.csv"],
)
def test_compare_columns(
    set_vsql_envs, capsys, root_project_dir, set_yb_envs, output_path
):
    pass
