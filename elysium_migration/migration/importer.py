import glob
import os
import shutil
from datetime import datetime
from pathlib import Path

from elysium_migration import Logger
from elysium_migration.migration.execute import Execution
from elysium_migration.migration.utility import MigrationUtility
from elysium_migration.configuration.constants import ConstantCatalog


class Importer:
    """
    Class for importing database objects 
    
    Args:
        import_objects (list[str]): The list of table names to import in 'schema.table' format
        input_dir (Path): The directory from which to import the data from.
        script_dir (Path): The directory where the scripts will be during execution. 
        validation_results_dir (Path): After the process completes, the validtion results will be in this directory. 

    Attributes:
        import_objects (list[str]): This is where we store the import objects.
        input_dir (Path): This is where we store the input_dir.
        script_dir (Path): This is where the script_dir is stored.  
        validation_results_dir (Path): This is where the validation_results_dir is stored.  
        null_character_errors (list[str]): This is the list of tables that have known null characters. This is not really used now.
        load_log_file_path (Path): This is where the path of the log file for the load utility is stored. 


    TODO: Abstract an interface per output system instead of just yellowbrick 
    """
    
    logger = None
    
    @classmethod
    def get_logger(cls):
        if cls.logger is None:
            cls.logger = Logger(log_name=__name__)
        return cls.logger
    
    def __init__(
        self,
        import_objects,
        input_dir: Path,
        script_dir: Path,
        validation_results_dir: Path,
    ):
        self.import_objects = import_objects
        self.input_dir = input_dir
        self.script_dir = script_dir
        self.validation_results_dir = validation_results_dir
        self.null_character_errors = self.import_objects["tables"]
        self.load_log_file_path = self.validation_results_dir / "logs" / "ybload"

    def import_tables(
        self, validate, field_delimiter=r"\t", extras="",
    ):
        """Imports data from the import directory into the destination database.

        Args:
            validate (bool): Whether or not to validate the import. This only works if the previous export had it on too. 
            field_delimiter (str): The field delimiter used in all the subsequent ybload commands.
            extras (str): This is for any command line arguments to be passed to the load utility.

        Raises:
            SystemExit: If there is no data in the directory to import from

        Returns:
            None

        """

        Importer.get_logger().log.info(
            "Initiating imports from {path}...".format(path=str(self.input_dir))
        )

        for schema_table in self.import_objects["tables"]:

            if not (self.load_log_file_path).exists:
                os.makedirs(self.load_log_file_path)

            fmt_str = "%Y%m%d%H%M%S"
            now_str = datetime.now().strftime(fmt_str)

            total_extras = f"{extras} --logfile {self.load_log_file_path}/{schema_table}_{now_str}.log --logfile-log-level DEBUG "
            p = self.input_dir / schema_table
            if not p.exists():
                Importer.get_logger.log.fatal(
                    f"{schema_table} directory of ingestion files '{p}' does not exist."
                )
                raise SystemExit(
                    f"{schema_table} directory of ingestion files '{p}' does not exist."
                )

            if ConstantCatalog.EXPORT_COMPRESSED:
                files = glob.glob(
                    f"{p}/*.{ConstantCatalog.DATA_FILES_EXTENSION}.{ConstantCatalog.DATA_COMPRESS_EXTENSION}
                )
            else:
                files = glob.glob(f"{p}/*.{ConstantCatalog.DATA_FILES_EXTENSION}")
                    
            files_batch_size = ConstantCatalog.IMPORT_FILES_BATCH_SIZE
            if ConstantCatalog.IMPORT_BATCH_FILES_FLAG and (
                len(files) > files_batch_size
            ):
                logger.log.debug(
                    f"Batch Importing {len(files)} {schema_table} files. IMPORT_BATCH_FILES_FLAG: True."
                )
                start = 0
                count = 1
                while True:

                    if start == (len(files)):
                        logger.log.info(
                            f"Table {schema_table} done importing all files."
                        )
                        break

                    files_remaining = len(files) - start
                    size_slice = min(files_batch_size, files_remaining)
                    end = start + size_slice
                    logger.log.debug(
                        f"Initiating {schema_table} import batch #{count}. End index: {end} Start index: {start} Number of files: {size_slice}."
                    )
                    files_slice = files[start:end]
                    files_expr = " ".join(files_slice)

                    Execution.ybload(
                        table=schema_table,
                        input_path=files_expr,
                        extras=total_extras,
                        field_delimiter=field_delimiter,
                    )

                    logger.log.debug(
                        f"Completed executing import of {len(files_slice)} {schema_table} files '{files_expr}'... "
                    )

                    count += 1
                    start = start + size_slice
            else:
                Importer.get_logger().log.debug(
                    f"Importing {len(files)} {schema_table} files {files}: IMPORT_BATCH_FILES_FLAG = False..."
                )

                if ConstantCatalog.EXPORT_COMPRESSED:
                    files = glob.glob(
                        f"{p}/*.{ConstantCatalog.DATA_FILES_EXTENSION}.{ConstantCatalog.DATA_COMPRESS_EXTENSION}
                    )
                else:
                    files = glob.glob(f"{p}/*.{ConstantCatalog.DATA_FILES_EXTENSION}")
                    
                files_expr = " ".join(files)
                Execution.ybload(
                    table=schema_table,
                    input_path=files_expr,
                    extras=total_extras,
                    field_delimiter=field_delimiter,
                )

                Importer.get_logger().log.info(f"Imported all {schema_table} files.")

        if validate:
            self._create_validation_results()

    def _pre_process_files_per_table(self, files, p):
        """Pre processes the files in the input directory. This is not used now. 

        Args:
            files (list[str]): This is the list of files to pre-process 
            p (Path): This is the directory of he data for this specific table 

        Raises:
            None

        Returns:
            None

        """

        for fi in files:
            logger.log.debug(
                f"Processing {fi}. Remove Nulls Flag: {null_character_errors_flag}."
            )

            self._pre_process_null_character_errors(fi, p, null_character_errors_flag)

    def _pre_process_null_character_errors(self, fi):
        """Pre-processes per file, removes the null bytes out of the file

        Args:
            fi (str): The path of the fiel being processed

        Raises:
            None

        Returns:
            None

        """
        in_file = open(fi, "r")
        lines = in_file.readlines()
        in_file.close()
        with open(fi, "w") as out_file:
            for line in lines:
                out_file.write(line.replace("\x00", ""))

    def _create_validation_results(self):
        """Creates the file with the results of the validation

        Args:
            None

        Raises:
            None

        Returns:
            None

        """
        root_project_dir = MigrationUtility.get_root_dir()
        val_results_dir = self.validation_results_dir
        if not val_results_dir.exists():
            os.makedirs(val_results_dir)

        val_queries_dir = root_project_dir / "migration" / "validation" / "queries"
        if val_queries_dir.exists():
            shutil.rmtree(val_queries_dir)
        os.makedirs(val_queries_dir)
        results = []
        val_script_path = (
            root_project_dir
            / "migration"
            / "validation"
            / "scripts"
            / "yb_exec_checksum.sh"
        )
        with open(val_script_path, "r") as f:
            lines = f.readlines()
            for i, command in enumerate(lines):
                Importer.get_logger().log.debug(
                    f"Executing validation command #{i} {command} from {val_script_path}... "
                )
                # TODO: This command is creating a '0' file here for some reason in the directory
                # that the cli is being executed - need to look into it
                output = Execution._execute(command)
                        
        fmt_str = "%Y%m%d%H%M%S"
        now_str = datetime.now().strftime(fmt_str)
        with open(
            val_results_dir / f"validation_script_results_{now_str}.log", "w"
        ) as f:
            for fi in glob.glob(str(val_queries_dir) + "/*.sql"):
                Importer.get_logger().log.debug(
                    f"Using validation sql {fi} as input to vsql..."
                )
                f.write(Execution.vsql(extra_output_args=f"-x < {fi}").decode() + "\n")
