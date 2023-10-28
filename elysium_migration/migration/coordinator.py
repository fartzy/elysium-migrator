import tempfile
from pathlib import Path
from typing import Optional

from elysium_migration import Logger
from elysium_migration.migration.config import Config, config
from elysium_migration.migration.exporter import Exporter
from elysium_migration.migration.importer import Importer
from elysium_migration.migration.utility import MigrationUtility


class ExportCoordinator:
    """Class for coordinating the entire export
    
        Args:
            None 

        Attributes:
            None
    """

    logger = None
    
    @classmethod
    def get_logger(cls):
        if cls.logger is None:
            cls.logger = Logger(log_name=__name__)
        return cls.logger
    
    @staticmethod
    def export(
        script_dir,
        config_file_path: Path,
        output_path: Path,
        inject_envs = False,
        sample_size,
        validate,
        from_date,
        to_date,
        env_dir,
    ):
        """This function does all the logic for exporting data 
        
            Args:
                script_dir (Path) : The directory where the scripts will be written to, or can be overriden if the user supplies it 
                config_file_path (Path) : The full path of the config file 
                output_path (Path): The path of the root directory where the data will be imported from 
                inject_envs (bool): The flag that can be driven by the user to inject from the 
                sample_size (int): When doing a sample migration, the use provides this as the amount of rows to use
                validate (bool): The boolean which flags if there will be validation or not
                from_date (str): The min date of the migration which is configured by the user 
                to_date (str): The max date of the migration which is configured by the user 
                env_dir (Path): Path of the .env file that the user can control 
                
            Returns:
                None
        """
        try:
            current_path = Path(__file__).parent.resolve()
            ExportCoordinator.get_logger().log.info(
                f"Executing `elysium-migrate-cli export` from '{current_path}'..."
            )

            MigrationUtility.write_scripts(env_dir)
        except Exception as e:
            ExportCoordinator.get_logger().log.fatal(
                f"Exception when writing scripts: {e}"
            )
            sys.exit(1)
        
        try:
            if inject_envs:
                Config.set_initial_envs(script_dir)
            config.set_from_yaml(config_file_path)
            export_objects = config.objects
        except Exception as e:
            ExportCoordinator.get_logger().log.fatal(
                f"Exception when loading configs: {e}"
            )            
            sys.exit(1)
            
        try:
            exporter = Exporter(
                export_objects=export_objects,
                output_dir=output_path,
                script_dir=script_dir
            )
            exporter.export_tables(
                sample_size=sample_size,
                validate=validate,
                from_date=from_date,
                to_date=to_date
            )
        except Exception as e:
            ExportCoordinator.get_logger().log.fatal(
                f"Exception when exporting data: {e}"
            )            
            sys.exit(1)
        
        
class ImportCoordinator:
    """Class for coordinating the entire import 
    
        Args:
            None 

        Attributes:
            None
    """
    
    logger = None
    
    @classmethod
    def get_logger(cls):
        if cls.logger is None:
            cls.logger = Logger(log_name=__name__)
        return cls.logger
    
    @staticmethod
    def import_data(
        script_dir,
        config_file_path: Path,
        output_path: Path,
        inject_envs = False,
        validate,
        val_dir,
        env_dir,
    ):
        """This function does all the logic for importing data 
        
            Args:
                script_dir (Path) : The directory where the scripts will be written to, or can be overriden if the user supplies it 
                config_file_path (Path) : The full path of the config file 
                output_path (Path): The path of the root directory where the data will be imported from 
                inject_envs (bool): The flag that can be driven by the user to inject from the 
                validate (bool): The boolean which flags if there will be validation or not
                val_dir (Path): Path where the validation results will be
                env_dir (Path): Path of the .env file that the user can control 

            Returns:
                None
        """
        try:
            current_path = Path(__file__).parent.resolve()
            ImportCoordinator.get_logger().log.info(
                f"Executing `elysium-migrate-cli import` from '{current_path}'..."
            )

            MigrationUtility.write_scripts(env_dir)
        except Exception as e:
            ImportCoordinator.get_logger().log.fatal(
                f"Exception when writing scripts: {e}"
            )
            sys.exit(1)
        
        try:
            if inject_envs:
                Config.set_initial_envs(str(script_dir))
            config.set_from_yaml(config_file_path)
            import_objects = config.objects
        except Exception as e:
            ImportCoordinator.get_logger().log.fatal(
                f"Exception when loading configs: {e}"
            )            
            sys.exit(1)
            
        try:
            importer = Importer(
                import_objects=import_objects,
                input_dir=output_path,
                script_dir=script_dir,
                validation_results_dir=val_dir,
            )
            importer.import_tables(validate=validate)
            
        except Exception as e:
            ExportCoordinator.get_logger().log.fatal(
                f"Exception when importing data: {e}"
            )            
            sys.exit(1)

        
