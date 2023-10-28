class ConstantCatalog:
    """This class provides constants and filters which should be moved to .ini file soon. 
    
    """
    
    VSQL_HOST = "VSQL_HOST"
    VSQL_USER = "VSQL_USER"
    VSQL_DATABASE = "VSQL_DATABASE"
    VSQL_PORT = "VSQL_PORT"
    VSQL_PASSWORD = "VSQL_PASSWORD"
    VSQL_PWORD_PROMPT = "\nEnter your vsql password: "

    YB_PASSWORD = "YBPASSWORD"
    YB_PWORD_PROMPT = "\nEnter your yellowbrick password: "
    YB_DATABASE = "YBDATABASE"
    YB_HOST = "YBHOST"
    YB_USER = "YBUSER"
    YB_NUM_READERS = 12
    YB_NUM_CORES = 32

    DATA_FILES_EXTENSION = "csv"
    DATA_COMPRESS_EXTENSION = "gz"
    IDEMPOTENT_EXPORT = True

    IMPORT_BATCH_FILES_FLAG = True
    IMPORT_FILES_BATCH_SIZE = 100

    EXPORT_COMPRESSED = True
    EXPORT_AS_CHUNKS_FLAG = False
    EXPORT_CHUNK_SIZE_MB = 5000
    EXPORT_WHOLE_TABLE_THRESHOLD_MB = 10000
    EXPORT_TABLE_WITH_WINDOW_THRESHOLD_MB = 20000
    EXPORT_WEEKS_WINDOW = 150

    @staticmethod
    def DATE_COL(table):
        """This function proveds a date column based on a schema. Basically it is a hardcoding 
        
        """

        sc, tbl = tuple(table.split("."))
        return "MartModifiedDate" if sc == "Compliance" else "CLOCK_TIMESTAMP"

    @staticmethod
    def DATE_FILTER(table_name):
        """This function provides a date_filter for metadata query. Basically it is a hardcoding 
        
        """
        # Just giving because the below function takes 10-15 seconds to return
        # Execution.vsql_get_sample_date_filter()

        date_filter = "2021-06-01 00:00:00"
        if table_name == "Elysium.FINGAM_Orders":
            date_filter = "2020-10-01 00:00:00"
        elif table_name == "Elysium.FINSECMM_Orders":
            date_filter = "2020-09-01 00:00:00"
        elif table_name == "Elysium.FINSECMM_Executions_T":
            date_filter = "2021-03-01 00:00:00"
        elif table_name == "sandbox.TKEY_LATEST_MAPPINGS_T":
            date_filter = "2017-04-01 00:00:00"
        return date_filter

    @staticmethod
    def YB_CHECK_SUM_PREDICATE(
        table="", part_col="", min_val="", max_val="", extra_predicate=""
    ):
        """This function gives a predicate that is just for sample predicate.  If there is no sample size given
            then the predicate will be handled in another places. Basically it is a hardcoding 
        """

        predicate = (
            "CLOCK_TIMESTAMP >= '2021-07-02 00:00:00' AND CLOCK_TIMESTAMP < '2021-07-09 00:00:00' "
            if table == "Elysium.BookmarkStore" and len(min_val) > 0
            else f"{part_col} >= '{str(min_val)}' AND {part_col} < '{str(max_val)}' "
            if len(min_val) > 0
            else ""
        ).lower()
        return predicate + extra_predicate

    @staticmethod
    def COALESCE_MIN_VAL(part_col=""):
        """This provides a value to coalesce to, it is based on data type. Basically it is a hardcoding 
        """
        return (
            "'1900-01-01 00:00:00'"
            if part_col in ["MartModifiedDate", "CLOCK_TIMESTAMP"]
            else 0
        )

    @staticmethod
    def TRUNCATE_STATEMENTS():
        """This obviously provides a hardcoding of delete from statements 
           TODO: Move this out of the constants and allow to be either Truncate or Delete based on permissions
           TODO: This needs to a loop of the config file tables and should be truncate if param is set
        """
        return """
            DELETE FROM Elysium.FINSECMM_Transactions;
            DELETE FROM Elysium.FINSECMM_Busts_T;
            DELETE FROM Elysium.FINSECMM_Orders;
            DELETE FROM Elysium.FINSECMM_Executions_T;
            DELETE FROM Elysium.FINSECMM_Enrichments;
            DELETE FROM Elysium.FINSECMM_Enrichments_Rejected;
            DELETE FROM Elysium.FINSECMM_Quotes;
            DELETE FROM Elysium.FINSECMM_ComplexTrades;
            DELETE FROM Elysium.TKey_Updates;
            DELETE FROM Elysium.TKey_Mapping_T;
            DELETE FROM Elysium.TKey_Cancel_T;
            DELETE FROM Elysium.TKey_Cancel_All_T;
            DELETE FROM Elysium.TKEY_LATEST_MAPPINGS_T;
            DELETE FROM Elysium.BookmarkStore;
            DELETE FROM Elysium.FINGAM_Transactions;
            DELETE FROM Elysium.FINGAM_Orders;
            DELETE FROM Elysium.FINGAM_Executions_T;
            DELETE FROM Elysium.FINGAM_Enrichments;
            DELETE FROM Elysium.FINGAM_Enrichments_Rejected;
            DELETE FROM Elysium.FINGAM_Busts_T;
            DELETE FROM Elysium.FINGAM_ComplexTrades;
            DELETE FROM Elysium.FINGAM_Orders_Rejected;
            DELETE FROM Elysium.Sweeper_Bookmark;	
            DELETE FROM sandbox.TKEY_LATEST_MAPPINGS_T;	
            DELETE FROM Compliance.DeskLevelUser;
            DELETE FROM Compliance.OverTheEdgeUser;
            DELETE FROM Compliance.OverTheEdgeUserDB;
            DELETE FROM Compliance.DeskLevelUserDB;
        """

    @staticmethod
    def NO_DATE_TABLES():
        return ["Elysium.Sweeper_Bookmark"]
