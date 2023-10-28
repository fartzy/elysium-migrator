import concurrent.futures
import inspect
import itertools
import json
import os
import shutil
from datetime import datetime
from pathlib import Path

import pytest

from tests.fixtures import (
    root_project_dir,
    sample_config_file,
    set_vsql_envs,
    setenvs,
    vertica_conn,
)
from elysium_migration import logger
from elysium_migration.configuration.constants import ConstantCatalog
from elysium_migration.migration.config import config
from elysium_migration.migration.execute import Execution, StatementCatalog
from elysium_migration.migration.exporter import Exporter


def test_schema_export_from_yaml(sample_config_file, db_connection):
    config.set_from_yaml(sample_config_file)
    export_objects = config.objects

    cursor = db_connection
    query = QueryCatalog.get_ddls_per_schema(export_objects["schema"])
    cursor.execute(query)

    rows = cursor.fetchall()

    for row in rows:
        log.info("db_row:" + row[0])
        assert row == None


def test_get_table_size(capsys, set_vsql_envs):
    schema_and_table = "Elysium.FINGAM_Enrichments"
    table_size = Execution.vsql_get_table_size_mb(table=schema_and_table)
    with capsys.disabled():
        print(table_size)

    assert int(table_size) > 1000


def test_coordinator_yaml_load(sample_config_file):
    err = Coordinator.export(sample_config_file)
    assert err == None


def test_min_val_retrieval():
    min_val_as_of_jul_8 = 8124008000001

    min_val = Execution.vsql_get_sample_filter_val(
        table="Elysium.FINGAM_Busts_T", sample_size=5000, part_col="ID"
    )


@pytest.mark.parametrize(
    "output_path",
    [
        "/logs/work/martz1/dev/elysium-migration-data/from-vertica/Elysium.FINGAM_Enrichments"
    ],
)
def test_whole_table_retrieval(capsys, set_vsql_envs, output_path):
    schema_and_table = "Elysium.FINGAM_Enrichments"
    table = "FINGAM_Enrichments"
    column = "ID"
    Execution.vsql(
        StatementCatalog.select_from_table(schema_and_table),
        f"{output_path}/999999999.csv",
        r"\t",
        "",
    )


@pytest.mark.parametrize(
    "output_path",
    [
        "/logs/work/martz1/dev/elysium-migration-data/from-vertica/Elysium.FINGAM_Enrichments"
    ],
)
def test_chunk_where_clause_retrieval(capsys, set_vsql_envs, output_path):
    schema_and_table = "Elysium.FINGAM_Enrichments"
    table = "FINGAM_Enrichments"
    column = "ID"

    chunk_size = Execution.vsql_get_chunk_size(table=schema_and_table)
    with capsys.disabled():
        print(chunk_size)
    where_clauses = Execution.vsql_get_chunk_where_clauses(
        table=schema_and_table, column=column, chunk_size=chunk_size
    )
    with capsys.disabled():
        print(where_clauses)

    numbered_clauses = zip(range(len(where_clauses)), where_clauses)
    before_export_time = datetime.now()
    with concurrent.futures.ThreadPoolExecutor(os.cpu_count() * 2) as executor:

        # Schedule the first N futures. We don't want to schedule them all
        # at once, to avoid consuming excessive amounts of memory

        futures = {
            executor.submit(
                Execution.vsql,
                StatementCatalog.select_from_table(
                    schema_and_table, predicate=predicate
                ),
                f"{output_path}/{str(id)}.csv",
                r"\t",
                "",
            )
            for id, predicate in itertools.islice(numbered_clauses, os.cpu_count() * 2)
        }

        while futures:
            done, futures = concurrent.futures.wait(
                futures, return_when=concurrent.futures.FIRST_COMPLETED
            )

            for fut in done:
                try:
                    after_table_export_time = datetime.now()
                    diff = after_table_export_time - before_export_time
                    logger.log.debug(
                        f"An export completed for {schema_and_table}. Time elapsed: {diff}"
                    )
                except Exception as e:
                    logger.log.error(e)

            for id, predicate in itertools.islice(numbered_clauses, len(done)):
                futures.add(
                    executor.submit(
                        Execution.vsql,
                        StatementCatalog.select_from_table(
                            schema_and_table, predicate=predicate
                        ),
                        f"{output_path}/{str(id)}.csv",
                        r"\t",
                        "",
                    )
                )
    after_whole_export_time = datetime.now()
    diff = after_whole_export_time - before_export_time
    logger.log.debug(
        f"Entire export completed for {schema_and_table}. Time elapsed: {diff}"
    )


def test_chunk_size_retrieval(capsys, set_vsql_envs):
    schema = "Elysium"
    table = "FINGAM_Enrichments"
    column = "ID"

    query = f"""
        WITH num_rows AS (
        SELECT schema_name,
            anchor_table_name AS table_name,
            SUM(total_row_count) AS ROWS
        FROM v_monitor.storage_containers sc
        JOIN v_catalog.projections p
            ON sc.projection_id = p.projection_id
            AND p.is_super_projection = TRUE
        GROUP BY schema_name,
                table_name,
                sc.projection_id
        )
        ,
        size_table AS (
        SELECT schema_name AS schema_name,
            anchor_table_name As table_name,
            round(SUM(used_bytes)/(1024^2), 1) AS used_mb,
            round(SUM(used_bytes)/1024, 2) AS used_kb
        FROM v_monitor.storage_containers sc
        JOIN v_catalog.projections p
            ON sc.projection_id = p.projection_id
        GROUP BY schema_name,
                table_name
        )
        ,
        row_counts AS (
        SELECT schema_name,
            table_name,
            MAX(ROWS) AS rows      
        FROM num_rows 
        GROUP BY schema_name,
                table_name
        )

        SELECT CAST(ROUND(100 / (used_mb / rows), -3) AS INT) chunk_size
        FROM row_counts rc
                JOIN size_table sz ON sz.table_name = rc.table_name
                        AND sz.schema_name = rc.schema_name
        WHERE rc.schema_name = '{schema}'
                AND sz.table_name = '{table}'
    """
    cli_output = Execution.vsql(query, extra_output_args="-t")
    with capsys.disabled():
        print(cli_output.decode().strip("\n\t "))

    assert int(cli_output.decode().strip("\n\t ")) == 617000


@pytest.mark.parametrize(
    "output_path",
    [
        "/logs/work/martz1/dev/elysium-migration-data/from-vertica/Elysium.TKey_Cancel_All_T/sample_1.csv"
    ],
)
def test_export_table_data(sample_config_file, set_vsql_envs, output_path):

    query = StatementCatalog.select_from_table(
        "Elysium.TKey_Cancel_All_T", " 1=1 LIMIT 5000"
    )

    # dont need to do anything with the output
    out = Execution.vsql(query, str(output_path))

    num_lines = 0
    fi = Path(output_path)
    if fi.exists():
        num_lines = sum(1 for line in open(output_path))

    # 5002 lines for the header lines
    assert num_lines == 5002


@pytest.mark.parametrize(
    "output_path",
    [
        "/logs/work/martz1/dev/elysium-migration-data/from-vertica/Elysium.FINSECMM_Orders/0.csv"
    ],
)
def test_export_table_data_delimiter(sample_config_file, set_vsql_envs, output_path):
    """
    Tables this was used on: 
        Elysium.FINGAM_Enrichments_Rejected ID>='383993000000'
        Elysium.FINGAM_Orders_Rejected ID>='1611831038890'
        Elysium.FINSECMM_Orders  ID>='189237019012'
        Elysium.FINSECMM_Enrichments_Rejected
    """
    schema_and_table = "Elysium.FINSECMM_Enrichments_Rejected"
    predicate = ""

    query = StatementCatalog.select_from_table(
        schema_and_table=schema_and_table, predicate=predicate,
    )

    out = Execution.vsql(
        query=query, output_path=str(output_path), field_delimiter="\t"
    )

    assert False


@pytest.mark.parametrize(
    "output_path", [Path("/logs/work/martz1/dev/elysium-migration-data/from-vertica")]
)
def test_export_all_tables(sample_config_file, set_vsql_envs, output_path):
    clear_dir(output_path)
    config.set_from_yaml(sample_config_file)
    export_objects = config.objects

    exporter = Exporter(export_objects, output_path)

    exporter.export_tables(sample_size=5000, validate=True)

    all_tables_have_out_files = True
    for k in exporter.table_partition_col_map.keys():
        p = Path(output_path) / k / "0.csv"
        if not p.exists():
            all_tables_have_out_files = False

    assert all_tables_have_out_files


def test_column_name_retreival(sample_config_file, set_vsql_envs, capsys):

    table = "Elysium.TKey_Updates"
    table_cols = [
        "ID",
        "CLOCK_TIMESTAMP",
        "VersionNumber",
        "UUID",
        "EventTime",
        "RecordType",
        "TxID",
        "TxVersionNumber",
        "InternalExecutionID",
        "OriginatorSystem",
        "BookingDate",
        "TxnVersionNumber",
        "TxnVersionTimestamp",
        "TxnVersionStatus",
        "ElysiumAMPSClientName",
        "ElysiumAMPSOrigin",
        "ElysiumAMPSTopic",
        "ElysiumAMPSYMDUUID",
        "FirmID",
        "OriginatorSubsystem",
        "ElysiumStartDateTime",
        "ElysiumRevisionNumber",
        "POExecutionID",
    ]

    sql = StatementCatalog.select_columns_from_table(table)
    query = Execution.vsql(query=sql, field_delimiter=",", extra_output_args="-t")

    cols = list(filter(lambda s: len(s) > 0, query.decode().split("\n")))

    cols_diff = [
        col for col in cols + table_cols if col not in table_cols or col not in cols
    ]

    assert cols_diff == []


@pytest.mark.parametrize(
    "output_path",
    [
        "/logs/work/martz1/dev/elysium-migration-data/from-vertica/Elysium.TKey_Cancel_All_T/0.csv"
    ],
)
def test_export_latest_sample(set_vsql_envs, output_path, capsys):

    table = "Elysium.TKey_Cancel_All_T"
    column = "Elysium.TKey_Cancel_All_T.ID".split(".")[2]

    sql = StatementCatalog.select_latest_table_sample(table, column, 5000)
    query = Execution.vsql(sql, output_path)

    assert Path(output_path).exists()


def test_get_test_where_clause(sample_config_file, set_vsql_envs, capsys):

    table = "Elysium.TKey_Cancel_All_T"
    column = "Elysium.TKey_Cancel_All_T.ID".split(".")[2]

    sql = StatementCatalog.get_test_where_clause(table, column, 5000)
    query = Execution.vsql(sql)

    # Min_val will be used to filter records
    # Any value larger than min_val will be selected
    min_val = Execution.vsql_get_sample_filter_val(
        table=k, sample_size=5000, part_col="ID"
    )

    assert min_val == "5261892024431"


def clear_dir(output_path):
    out_p = Path(output_path)
    if not out_p.exists():
        os.makedirs(out_p)
    else:
        for files in os.listdir(output_path):
            path = os.path.join(output_path, files)
            try:
                shutil.rmtree(path)
            except OSError:
                os.remove(output_path)


@pytest.mark.parametrize(
    "output_path", ["/logs/work/martz1/dev/elysium-migration-data/from-vertica/"],
)
def test_full_export_with_where_clause(
    output_path, sample_config_file, set_vsql_envs, capsys, root_project_dir
):

    config.set_from_yaml(sample_config_file)
    export_objects = config.objects
    exporter = Exporter(export_objects, output_path)
    tbls = exporter.table_partition_col_map

    logger.log.debug(
        f"tbls being tested in {inspect.currentframe().f_code.co_name}:{tbls.keys()}"
    )

    # check for existnece of directory and clear it out
    clear_dir(output_path)

    d = {}
    # for k, v in {"Elysium.FINSECMM_Quotes": "ID"}.items():
    for k, v in tbls.items():
        predicate = ""

        # Admins named all the tables lowercase for some reason nd the database is case sensitive
        sc, tbl = tuple(k.split("."))

        ysc, ytbl = sc.lower(), tbl.lower()
        ydb = os.environ[ConstantCatalog.YB_DATABASE].lower()

        if v:
            min_val = Execution.vsql_get_sample_filter_val(
                table=k, sample_size=5000, part_col=v
            )
            predicate = ConstantCatalog.YB_CHECK_SUM_PREDICATE(
                table=k, part_col=v, min_val=min_val
            )

            logger.log.debug(
                f"""{inspect.currentframe().f_code.co_name} debug info: "
                        sc, tbl, k, ydb : {sc}, {tbl}, {k}, {ydb}"
                        {k} predicate is : {predicate}"
                        min_val : {min_val}"""
            )

            validation_queries_path = (
                root_project_dir / "tests" / "sql" / "validation" / "queries"
            )

            if not validation_queries_path.exists():
                os.makedirs(validation_queries_path)

            d_val = f"""{root_project_dir}/scripts/yb_checksum.sh {ydb} {ysc} {ytbl} \"where {ytbl}.{predicate}\" > {validation_queries_path}/{ysc}_{ytbl}.sql"""

        else:
            d_val = f"""{root_project_dir}/scripts/yb_checksum.sh {ydb} {ysc} {ytbl} > {validation_queries_path}/{ysc}_{ytbl}.sql"""

        d[k] = d_val

        p = Path(output_path) / k
        if not p.exists():
            os.makedirs(p)

        order_by_col = "" if v is None else v

        Execution.vsql(
            StatementCatalog.select_from_table(
                schema_and_table=k, predicate=predicate, col_order_by_desc=order_by_col
            ),
            output_path=f"{p}/0.csv",
            field_delimiter=r"\t",
        )

    # with capsys.disabled():
    #     print("commands for yb_checksum:\n\n")
    #     for k, v in d.items():
    #         print(v)

    yb_val_script = root_project_dir / "tests/scripts/yb_val.sh"
    with open(yb_val_script, "w") as f:
        for k, v in d.items():
            f.write(v + "\n")

    all_tables_have_out_files = True
    for k in exporter.table_partition_col_map.keys():
        p = Path(output_path) / k / "0.csv"
        if not p.exists():
            all_tables_have_out_files = False

    assert all_tables_have_out_files


@pytest.mark.parametrize(
    "output_path", ["/logs/work/martz1/dev/elysium-migration-data/from-vertica/"],
)
def test_generate_yb_val_scripts(
    output_path, sample_config_file, set_vsql_envs, capsys, root_project_dir
):
    config.set_from_yaml(sample_config_file)
    export_objects = config.objects
    exporter = Exporter(export_objects, output_path)
    tbls = exporter.table_partition_col_map

    d = {}
    for k, v in tbls.items():
        predicate = ""

        # yb admins named all the tables lowercase so we need the lower()
        sc, tbl = tuple(k.split("."))

        ysc, ytbl = sc.lower(), tbl.lower()
        ydb = os.environ[ConstantCatalog.YB_DATABASE].lower()

        if v:
            min_val = Execution.vsql_get_sample_filter_val(
                table=k, sample_size=5000, part_col=v
            )
            predicate = ConstantCatalog.YB_CHECK_SUM_PREDICATE(
                table=k, part_col=v, min_val=min_val
            )

            logger.log.debug(
                f"""{inspect.currentframe().f_code.co_name} debug info: "
                        sc, tbl, k, ydb : {sc}, {tbl}, {k}, {ydb}"
                        {k} predicate is : {predicate}"
                         min_val : {min_val}"""
            )

            validation_queries_path = (
                root_project_dir / "tests" / "sql" / "validation" / "queries"
            )

            if not validation_queries_path.exists():
                os.makedirs(validation_queries_path)

            d_val = f"""{root_project_dir}/scripts/yb_checksum.sh {ydb} {ysc} {ytbl} \"where {ytbl}.{predicate}\" > {validation_queries_path}/{ysc}_{ytbl}.sql"""

        else:
            d_val = f"""{root_project_dir}/scripts/yb_checksum.sh {ydb} {ysc} {ytbl} > {validation_queries_path}/{ysc}_{ytbl}.sql"""

        d[k] = d_val

    yb_val_script = root_project_dir / "tests/scripts/yb_val.sh"
    with open(yb_val_script, "w") as f:
        for k, v in d.items():
            f.write(v + "\n")

    assert False
