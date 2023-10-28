import concurrent.futures
import itertools
import os
from datetime import datetime
from pathlib import Path

from elysium_migration import Logger
from elysium_migration.configuration.constants import ConstantCatalog
from elysium_migration.migration.execute import Execution, StatementCatalog
from elysium_migration.migration.utility import MigrationUtility


class Exporter:
    """
    Class for exporting database objects 
    
    Args:
        export_objects (list[str]): The list of table names to export to their own directory in 'schema.table' format
        output_dir (Path): The parent directory to export the data to.
        script_dir (Path): The directory where the scripts will be during execution. 
        validation_results_dir (Path): After the process completes, the validtion results will be in this directory. 

    Attributes:
        export_objects (list[str]): This is where we store the export_objects.
        input_dir (Path): This is where we store the output_dir.
        script_dir (Path): This is where the script_dir is stored.  
        checksum_file_path (Path): The file path of the file with the yb_checksum commands.  
        val_sql_path (Path): This is the path of the directory where validation sql files will be stored.
        table_partition_col_map (dict[str:str]): This stores each table and its respective partitioning column.


    TODO: Abstract an interface per output system instead of just yellowbrick 
    """
    
    logger = None
    
    @classmethod
    def get_logger(cls):
        if cls.logger is None:
            cls.logger = Logger(log_name=__name__)
        return cls.logger
    
    def __init__(self, export_objects, output_dir: Path, script_dir):
        self.export_objects = export_objects
        self.output_dir = output_dir
        self.script_dir = script_dir
        self.checksum_file_path = self._clear_checksum_file()
        self.val_sql_path = self._clear_sql_val_path()

    @property
    def table_partition_col_map(self):
        """This property has a maping of tables to partition column name. 
            The partition column name should be in form <schema>.<table>.<column_name>
        
        """
        tbls = self.export_objects["tables"]
        d = {
            ".".join(s.split(".")[:2]): s.split(".")[2]
            for s in self.export_objects["partitioning_columns"]
        }
        d.update({tbl: None for tbl in tbls if tbl not in d.keys()})

        logger.log.debug(
            """Loaded the "Table"->"Partition Column" map from the configuration file:\n\t{vals}""".format(
                vals="\n\t".join([k + ":" + str(v) for k, v in d.items()])
            )
        )

        return d

    def export_tables(
        self, sample_size=5000, validate=False, from_date=None, to_date=None
    ):
        """Export to the output directory, given the export type will determine the output dir structure
            Each subdirectory in the output directory will have th

        Arguments:
            sample_size (int): This is the size of the sample if we just want to sample the latest date. 
                The cli default is a 0 so the default will pass in a 0 to override the default here.
            validate (bool): flag for whether or not to do validation.
            from_date (str): A date string in ISO 8601 format that determins the minimum date.
            to_date (str): A date string in ISO 8601 format that determins the maximum date.

        Raises:
            None
        Returns:
            None

        TODO: Make an enum export_type that signifies the export of a whole schema possibly
        TODO: Handle other export_type (for example the whole schema)

        TODO: There is not yet any exception handling of FS ops 
        TODO: A more graceful class for predicate building other than string concatenation
        """

        # The behavior will be a little different if there is no 'part_col' defined.
        # There won't be a predicate at all, so there will be no 'min_val' either.
        MigrationUtility.clear_dir(self.output_dir)
        tbls_cols = self.table_partition_col_map.items()
        for tbl, part_col in tbls_cols:
            before_export_table = datetime.now()
            table_output_path = self.output_dir / tbl
            MigrationUtility.clear_dir(table_output_path)

            Exporter.get_logger().log.info(
                f"Export beginning for {tbl} @ {before_export_table}."
            )
            # if sample_size is > 0, then these values will stay blank for remainder
            # of this function. sample_size of zero implies the whole table will be moved
            min_val = predicate = order_by_col = ""
            if sample_size > 0:
                if part_col:
                    min_val = Execution.vsql_get_sample_filter_val(
                        table=tbl, part_col=part_col, sample_size=sample_size
                    )

                    max_val = Execution.vsql_get_max_col_val(table=tbl, column=part_col)

                    predicate = ConstantCatalog.YB_CHECK_SUM_PREDICATE(
                        table=tbl, part_col=part_col, min_val=min_val, max_val=max_val
                    )

                    order_by_col = part_col

                    if validate:
                        self._write_checksum_command(
                            tbl=tbl,
                            part_col=part_col,
                            sample_max_val=max_val,
                            sample_min_val=min_val,
                        )

                Execution.vsql(
                    StatementCatalog.select_from_table(
                        schema_and_table=tbl,
                        predicate=predicate,
                        col_order_by_desc=order_by_col,
                    ),
                    output_path=f"{str(table_output_path)}/0.{ConstantCatalog.DATA_FILES_EXTENSION}",
                    field_delimiter=r"\t",
                )

            elif to_date or from_date:
                table_size_mb = Execution.vsql_get_table_size_mb(table=tbl)
                date_col = ConstantCatalog.DATE_COL(table=tbl)
                no_date_tables = ConstantCatalog.NO_DATE_TABLES()

                weeks_in_window = -1
                if tbl in no_date_tables:
                    predicate = ""
                elif all([to_date, from_date]):
                    predicate = (
                        f"{date_col} >= '{from_date}' AND {date_col} < '{to_date}'"
                    )
                    fmt_str = "%Y-%m-%d %H:%M:%S"
                    window = datetime.strptime(to_date, fmt_str) - datetime.strptime(
                        from_date, fmt_str
                    )
                    weeks_in_window = window.total_seconds() / 60 / 60 / 24 / 7
                elif not to_date:
                    predicate = f"{date_col} >= '{from_date}'"
                else:
                    predicate = f"{date_col} < '{to_date}'"

                Exporter.get_logger().log.debug(
                    f"{tbl} predicate based on user provided input dates: '{predicate}'."
                )
                
                # This delete statement makes the load idempotent
                if ConstantCatalog.IDEMPOTENT_EXPORT:
                    Execution.ybsql_delete_date_range(
                        table=tbl, predicate=f" AND {predicate}"
                    )
                
                # Dont need to break up the data in chunks if it is just a small table.
                # Check and see if table is not over either 10GB and window is 2 years or less
                # OR if table is just under 2 GB. This is just a roough estimate
                if (
                    (
                        int(table_size_mb)
                        < ConstantCatalog.EXPORT_TABLE_WITH_WINDOW_THRESHOLD_MB
                        and (
                            weeks_in_window < ConstantCatalog.EXPORT_WEEKS_WINDOW
                            and weeks_in_window > 0
                        )
                    ) 
                    or (
                        int(table_size_mb) 
                        < ConstantCatalog.EXPORT_WHOLE_TABLE_THRESHOLD_MB
                    ) 
                    or ( ConstantCatalog.EXPORT_AS_CHUNKS_FLAG == False )
                ):
                    Exporter.get_logger().log.debug(
                        f"""{tbl} not chunking becasue mb size is only {table_size_mb} and the date range is '{'Not Set' if weeks_in_window == -1 else str(weeks_in_window)}' weeks.
                        EXPORT_AS_CHUNKS_FLAG has been set to '{ConstantCatalog.EXPORT_AS_CHUNKS_FLAG}'."""
                    )
                    
                    if ConstantCatalog.EXPORT_COMPRESSED == True:
                        Execution.vsql(
                            StatementCatalog.select_from_table(
                                schema_and_table=tbl,predicate=f" AND {predicate}"
                            ),
                            output_path=f"{table_output_path}/0.{ConstantCatalog.DATA_FILES_EXTENSION}.{ConstantCatalog.DATA_COMPRESS_EXTENSION}",
                            field_delimiter=r"\t",
                            compressed=True,
                        )
                    else:
                    
                        Execution.vsql(
                            StatementCatalog.select_from_table(
                                schema_and_table=tbl,
                                predicate=predicate,
                                col_order_by_desc=order_by_col,
                            ),
                            output_path=f"{table_output_path}/0.{ConstantCatalog.DATA_FILES_EXTENSION}",
                            field_delimiter=r"\t",
                        )
                else:
                    Exporter.get_logger().log.debug(
                        f"{tbl} is chunking because the data mb is too big '{table_size_mb}' and the date range is '{'Not Set' if weeks_in_window == -1 else str(weeks_in_window)}' weeks."
                    )
                    chunk_size = Execution.vsql_get_chunk_size(table=tbl)
                    Exporter.get_logger().log.debug(
                        f"Each {tbl} file will have a chunk of '{chunk_size}' records."
                    )
                    where_clauses = Execution.vsql_get_chunk_where_clauses(
                        table=tbl,
                        column=part_col,
                        chunk_size=chunk_size,
                        predicate=f" AND {predicate}",
                    )

                    date_filtered_where_clauses = [
                        clause + " AND " + predicate for clause in where_clauses
                    ]
                    numbered_clauses = zip(
                        range(len(where_clauses)), date_filtered_where_clauses
                    )
                    total_rows = int(chunk_size) * len(where_clauses)
                    Exporter.get_logger().log.debug(
                        f"""Up to {total_rows} {tbl} rows to export. Date window = "{predicate or 'None'}"."""
                    )
                    
                    self.parallelize_table_export(
                        numbered_clauses=numbered_clauses,
                        schema_and_table=tbl,
                        output_path=str(table_output_path),
                    )

                if validate:
                    self._write_checksum_command(
                        tbl=tbl, part_col=part_col, extra_predicate=predicate
                    )

            # if sample size is zero, then move the whole table
            else:
                chunk_size = Execution.vsql_get_chunk_size(table=tbl)
                where_clauses = Execution.vsql_get_chunk_where_clauses(
                    table=tbl, column=column, chunk_size=chunk_size
                )
                numbered_clauses = zip(range(len(where_clauses)), where_clauses)
                table_size_mb = Execution.vsql_get_table_size_mb(table=tbl)

                # if table is less than two gigabyte, really no need to break it up into chunks
                # there is no need to apply any where filter either
                if int(table_size_mb) < ConstantCatalog.EXPORT_WHOLE_TABLE_THRESHOLD_MB:
                    Execution.vsql(
                        StatementCatalog.select_from_table(
                            schema_and_table=tbl,
                            predicate=predicate,
                            col_order_by_desc=order_by_col,
                        ),
                        output_path=f"{str(table_output_path)}/0.{ConstantCatalog.DATA_FILES_EXTENSION}",
                        field_delimiter=r"\t",
                    )

                else:
                    Exporter.get_logger().log.debug(f"""All {tbl} rows being exported.""")
                    self.parallelize_table_export(
                        numbered_clauses=numbered_clauses,
                        schema_and_table=tbl,
                        output_path=str(table_output_path),
                    )

                if validate:
                    self._write_checksum_command(tbl=tbl, part_col=part_col)

            export_time = datetime.now() - before_export_table
            Exporter.get_logger().log.info(
                f"Table {tbl} export finished. Time elapsed: {export_time}."
            )

    def parallelize_table_export(self, numbered_clauses, schema_and_table, output_path):
        """Distributes the extraction of data to other threads. Keeps the worker pool full of threads.

        Arguments:
            numbered_clauses (list[(int, string)]): This has an int for numbering the files and a string
                which is the WHERE clause of the query.
            schema_and_table (str): The name of the table in shcema.table format.
            output_path (str): output_path fo files in str ISO 8601 format

        Raises:
            IOException
        Returns:
            None
        """
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
                    f"{output_path}/{str(id)}.{ConstantCatalog.DATA_FILES_EXTENSION}",
                    r"\t",
                    "",
                )
                for id, predicate in itertools.islice(
                    numbered_clauses, os.cpu_count() * 2
                )
            }

            while futures:
                done, futures = concurrent.futures.wait(
                    futures, return_when=concurrent.futures.FIRST_COMPLETED
                )

                for fut in done:
                    try:
                        after_table_export_time = datetime.now()
                        diff = after_table_export_time - before_export_time
                        Exporter.get_logger().log.debug(
                            f"An export completed for {schema_and_table}. Total Time elapsed: {diff}."
                        )
                    except Exception as e:
                        Exporter.get_logger().log.error(e)

                for id, predicate in itertools.islice(numbered_clauses, len(done)):
                    futures.add(
                        executor.submit(
                            Execution.vsql,
                            StatementCatalog.select_from_table(
                                schema_and_table, predicate=predicate
                            ),
                            f"{output_path}/{str(id)}.{ConstantCatalog.DATA_FILES_EXTENSION}",
                            r"\t",
                            "",
                        )
                    )
        after_whole_export_time = datetime.now()
        diff = after_whole_export_time - before_export_time
        Exporter.get_logger().log.debug(
            f"Entire export completed for {schema_and_table}. Total Time elapsed: {diff}."
        )

    def _clear_checksum_file(self):
        """Clears the path for the checksum file.

        Arguments:
            None
        Raises:
            None
        Returns:
            cheksum (Path): Path of checksum commands
        """
        checksum_exec_path = (
            MigrationUtility.get_root_dir() / "migration" / "validation" / "scripts"
        )
        MigrationUtility.clear_dir(checksum_exec_path)
        with open(checksum_exec_path / "yb_exec_checksum.sh", "w") as f:
            pass
        return checksum_exec_path / "yb_exec_checksum.sh"

    def _clear_sql_val_path(self):
        """Clears the path for the validation queries.

        Arguments:
            None
        Raises:
            None
        Returns:
            val_path (Path): Path of validation queries.
        """
        val_sql_path = (
            MigrationUtility.get_root_dir() / "migration" / "validation" / "queries"
        )

        MigrationUtility.clear_dir(val_sql_path)
        return val_sql_path

    def _create_checksum_command(
        self,
        tbl,
        part_col,
        val_sql_path,
        sample_min_val="",
        sample_max_val="",
        extra_predicate="",
    ):
        """Creates the actual command of the checksum.

        Arguments:
            tbl (str): Table name.
            part_col (str): The partition column. 
            val_sql_path (str): Tha path of the validation queries. 
            sample_min_val (str): The lowest boundary of the sample query.
            sample_max_val (str): The highest boundary of the sample query.
            extra_predicate (str): Any extra preciates to be passed in for the checksum command.
        Raises:
            None
        Returns:
            checksum_command (str): The completely formatted string of the checksum command. 
        """
        yb_checksum = str(Path(self.script_dir) / "yb_checksum.sh")

        # TODO: Construct abstraction from below yellowbrick env-variable dependency
        ydb = os.environ[ConstantCatalog.YB_DATABASE]

        ysc, ytbl = tuple(tbl.lower().split("."))

        where_clause = ""

        if part_col:
            if len(sample_min_val) > 0 or len(extra_predicate) > 0:
                predicate = ConstantCatalog.YB_CHECK_SUM_PREDICATE(
                    table=tbl,
                    part_col=part_col,
                    min_val=sample_min_val,
                    max_val=sample_max_val,
                    extra_predicate=extra_predicate,
                )
                where_clause = f"""\"where {predicate}\""""

        Exporter.get_logger().log.debug(
            f"Creating the checksum command:  Checksum script: {yb_checksum}, Database: {ydb}, Schema: {ysc}, Table: {ytbl}, Predicate: {where_clause}..."
        )
        return f"""{yb_checksum} {ydb} {ysc} {ytbl} {where_clause} > {val_sql_path}/{ysc}_{ytbl}.sql"""

    def _write_checksum_command(
        self, tbl, part_col, sample_min_val="", sample_max_val="", extra_predicate=""
    ):
        """Writes the checksum commands to disk. To be executed later. 

        Arguments:
            tbl (str): Table name.
            part_col (str): The partition column. 
            sample_min_val (str): The lowest boundary of the sample query.
            sample_max_val (str): The highest boundary of the sample query.
            extra_predicate (str): Any extra preciates to be passed in for the checksum command.
        Raises:
            None
        Returns:
            None
        """
        Exporter.get_logger().log.debug(
            f"Writing checksum commands to disk: Table: {tbl}, Partition Column: {part_col}, Min Sample Val: {sample_min_val}, Max Sample Val:{sample_max_val}, Extra predicate: {extra_predicate}..."
        )

        if len(sample_min_val) == 0 and len(sample_max_val) == 0:

            command = self._create_checksum_command(
                tbl=tbl,
                part_col=part_col,
                val_sql_path=self.val_sql_path,
                sample_min_val=sample_min_val,
                sample_max_val=sample_max_val,
                extra_predicate=extra_predicate,
            )

        else:
            command = self._create_checksum_command(
                tbl=tbl,
                part_col=part_col,
                val_sql_path=self.val_sql_path,
                sample_min_val=sample_min_val,
                sample_max_val=sample_max_val,
            )

        with open(self.checksum_file_path, "a") as f:
            Exporter.get_logger().log.debug(
                f"Writing checksum command '{command}' to '{str(self.checksum_file_path)}'..."
            )
            f.write(command + "\n")
