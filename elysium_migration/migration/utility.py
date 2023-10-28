import os
import shutil
from pathlib import Path

import elysium_migration
from elysium_migration import logger
from elysium_migration.migration.execute import ScriptCatalog


class MigrationUtility:
    """Utility class used in other parts of the application
    
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
    def clear_dir(output_path):
        """Clears the directory and creates if it doesnt exist.

        Args:
            output_path (str): the path to be cleared or created.

        Raises:
            OSError, NotADirectoryError

        Returns:
            None

        """
        out_p = Path(output_path)
        if not out_p.exists():
            os.makedirs(out_p)
        else:
            for files in os.listdir(output_path):
                path = os.path.join(output_path, files)
                try:
                    shutil.rmtree(path)
                except (OSError, NotADirectoryError):
                    os.remove(path)

        MigrationUtility.get_logger().log.debug(
            f"Directory '{output_path}' is now empty and available."
        )

    @staticmethod
    def get_root_dir():
        path = Path(elysium_migration.__path__[0])
        return path

    @staticmethod
    def write_scripts(env_dir_path):
        """Writes the scripts out to the script directory.

        Args:
            env_dir_path (Path): the path of the .env file.

        Raises:
            None
            
        Returns:
            None

        """
        scripts_path = MigrationUtility.get_root_dir() / "scripts"
        getenv = str(scripts_path / "getenv.sh")
        yb_checksum = str(scripts_path / "yb_checksum.sh")
        MigrationUtility.get_logger().log.debug(
            f"Writing scripts to local scripts directory: '{str(scripts_path)}'..."
        )
        if not scripts_path.exists():
            os.makedirs(scripts_path)
        with open(getenv, "w") as f:
            f.write(ScriptCatalog.getenvs_script(env_dir_path))

        with open(yb_checksum, "w") as f:
            f.write(ScriptCatalog.yb_checksum_script())

        os.chmod(getenv, 0o777)
        os.chmod(yb_checksum, 0o777)
