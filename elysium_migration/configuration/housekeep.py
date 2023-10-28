from elysium_migration.migration.config import Config
from elysium_migration.migration.execute import Execution
from elysium_migration.migration.utility import MigrationUtility


class HouseKeeper:
    """Class for supplying methods for cleaning up migrations or prepping migrations 
    
        Args:
            None 

        Attributes:
            None
    """

    @staticmethod
    def work(
        inject_envs_from_env_file, truncate_tables, log_cleanup, script_dir, env_dir
    ):
        MigrationUtility.write_scripts(env_dir)
        if inject_envs_from_env_file:
            Config.set_initial_envs(script_dir=script_dir)
        if truncate_tables:
            HouseKeeper.truncate_tables()

    @staticmethod
    def truncate_tables():
        Execution.ybsql_truncate_tables()
