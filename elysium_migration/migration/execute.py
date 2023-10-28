import os
import subprocess

from elysium_migration import Logger
from elysium_migration.configuration import Platform
from elysium_migration.configuration.constants import ConstantCatalog


class StatementCatalog:
    """Class for holding static methods to create statements for execution
    
    Args:
        None 

    Attributes:
        None
    """

    @staticmethod
    def get_table_ddls_from_schema(schemas):
        """Get the statement for extraction of all DDL of tables for specified schemas

        Arguments:
            schemas (str): This is a comman delimted list of schemas to get all of that schema table DDL 

        Raises:
            None
        Returns:
            query (str): A query for VSQL to get DDL for specified database objects
        """

        return """
        SELECT EXPORT_TABLES('','{schemas}')
        """.format(
            schemas=",".join(schemas)
        )

    @staticmethod
    def select_from_table(
        schema_and_table, predicate="", limit=0, col_order_by_desc="", cols_expr="*"
    ):
        """Select and return data from a specific table 

        Arguments:
            schema_and_table (str): the table to be queried 
            predicate (str): an optioanl additional predicate to be applied
            limit (str): an optional limit to be applied to query
            col_order_by_desc (str): An optional column to order by 
            cols_expr (str): An opitonal str to give which has a column delimited list of column expressions

        Returns:
            query (str): A query to get data from the specified table 
        """
        # TODO: Find a better way of building complex predicates
        #   This could potentially cause 'and to be entered many times

        if predicate:
            if predicate.strip()[:3].lower() != "and":
                predicate = f" AND {predicate}"

        lim_str = ""
        if limit > 0:
            lim_str = f" LIMIT {str(limit)}"

        order_by = col_order_by_desc
        if (len(col_order_by_desc) > 0) and (
            "order by" not in col_order_by_desc.lower()
        ):
            order_by = f"\nORDER BY {col_order_by_desc} DESC "

        return f"""
        SELECT {cols_expr} FROM {schema_and_table} WHERE 1=1 {predicate} {order_by} {lim_str}
        """

    @staticmethod
    def select_latest_table_sample(schema_and_table, column, limit=0):
        """Select and return sample non determinsitc data from a specific table 

        Arguments:
            schema_and_table (str): the table to be queried 
            col (str): The column to rank the query by
            limit (str): an optional limit to be applied to query
            
        Raises:
            None
        Returns:
            query (str): A query to get data from the specified table 
        """
        cols_expr = ", ".join(
            Execution.get_table_column_names(
                schema_and_table=schema_and_table, platform=Platform.VERTICA
            )
        )

        return f"""
        with cte1 AS (
            SELECT RANK() OVER (ORDER BY {column} DESC) AS Ranking
                , {cols_expr}
            FROM {schema_and_table}
        )

        SELECT {cols_expr} 
        FROM cte1 WHERE Ranking <= {limit}
        """

    @staticmethod
    def get_table_size_mb(table):
        """For vertica only - gets the table size in mb

        Arguments:
            schema_and_table (str): the table to get the data size for 
            
        Raises:
            None
        Returns:
            query (str): A query to get the table size in mb 
        """
        schema, table = tuple(table.split("."))
        return f"""
        SELECT round(SUM(used_bytes)/(1024^2), -2)
        FROM v_monitor.storage_containers sc
        JOIN v_catalog.projections p
            ON sc.projection_id = p.projection_id
        WHERE schema_name = '{schema}'
        AND anchor_table_name = '{table}'
        GROUP BY schema_name,
                anchor_table_name
        UNION ALL
        SELECT 0 
        ORDER BY 1 DESC
        LIMIT 1
        
        """

    @staticmethod
    def truncate_tables():
        """Truncates the tables - uses a hard coded from ConstantCatalog

        Arguments:
            None
            
        Raises:
            None
        Returns:
            query (str):  The delete from queries 
        """
        return ConstantCatalog.TRUNCATE_STATEMENTS()

    @staticmethod
    def delete_date_range(table, predicate):
	"""Creates a delete statement that will delete the records from the target before loading
	
	Arguments:
	    table (str): The table name from which to delete
	    predicate (str): The where clause of the delete statement
	    
	Returns:
	    None
	"""
	return f"""
	    DELETE 
	    FROM {table}
	    WHERE 1=1
	        {predicate}
	"""
	    

    @staticmethod
    def get_sample_date_filter(schema_and_table, part_col, date_col, sample_size):
        """Gets the query for the date filter to apply for the get_chunk_size query

        Arguments:
            schema_and_table (str): The name of the table
            part_col(str): The paritionaing column 
            date_col(str): The date column 
            sample_size(str): The size of the sampling 
            
        Returns:
            query (str):  The query to get the sample date filter 
        """

        sample_size_triple = sample_size * 3

        return f"""
            SELECT MIN({date_col}) 
            FROM 
                (
                    SELECT {date_col} 
                    FROM {schema_and_table}
                    ORDER BY {part_col} DESC
                    LIMIT {sample_size_triple}
                ) a
        """

    @staticmethod
    def get_chunk_size(
        schema_and_table, chunk_size_mb=ConstantCatalog.EXPORT_CHUNK_SIZE_MB
    ):
        """Gets the query to get the chunk size based on desired output file size

        Arguments:
            schema_and_table (str): The name of the table
            chunk_size_mb(str): The size in mb of desired output file 
            
        Returns:
            query (str):  The query to get the chunk size
        """
        schema, table = tuple(schema_and_table.split("."))

        return f"""
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

            SELECT CAST(ROUND({chunk_size_mb} / (used_mb / rows), -3) AS INT) chunk_size
            FROM row_counts rc
                    JOIN size_table sz ON sz.table_name = rc.table_name
                            AND sz.schema_name = rc.schema_name
            WHERE rc.schema_name = '{schema}'
                    AND sz.table_name = '{table}'
        """

    @staticmethod
    def get_chunk_where_clauses(schema_and_table, column, chunk_size, predicate=""):
        """Returns the query to get all the where clauses as predicates for the queries after getting chunk size

        Arguments:
            schema_and_table (str): The name of the table
            column (str): The name of the partitioning column
            chunk_size_mb (str): The size in mb of desired output file 
            predicate (str):   The extra predicate to add the the output query
            
        Returns:
            query (str):  The query itself 
        """

        coalesce_val = ConstantCatalog.COALESCE_MIN_VAL(column)
        return f"""
        with row_nums AS (
                SELECT {column}
                    , ROW_NUMBER() OVER (ORDER BY {column} ASC) AS row_num  
                FROM {schema_and_table}
                WHERE 1 = 1
                  {predicate}
            )
            ,
            chunks AS (
                SELECT row_num
                    , {column}
                FROM row_nums
                WHERE row_num % {chunk_size} = 0 
            )
            ,
            predicates AS (
                SELECT row_num as row_num
                    , LAG({column}, 1) OVER (ORDER BY row_num) AS FROM_{column} 
                    , LEAD({column}, 1) OVER (ORDER BY row_num) AS DUMMY_{column}  
                    , {column} AS TO_{column} 
                FROM chunks
            )

            SELECT '{column} >= ' || COALESCE(FROM_{column}, {coalesce_val}) || 
                            CASE 
                                WHEN DUMMY_{column} IS NULL 
                                    THEN '' 
                                ELSE  ' AND {column} < ' || TO_{column} 
                            END
            FROM predicates
        """

    @staticmethod
    def get_test_where_clause(
        schema_and_table,
        column,
        sample_size=0,
        date_filter="2021-06-15 00:00:00",
        date_col="MartModifiedDate",
    ):
        """Returns the query to get the test where clause 

        Arguments:
            schema_and_table (str): The name of the table
            column (str): The name of the partitioning column
            sample_size (str): An optional sample size 
            predicate (str): The date filter to help with the latenc of this query execution
            date_col (str):
            
        Returns:
            query (str):  The query to get the sample date filter 
        """

        return f"""
            WITH cte1 AS (
                SELECT RANK() OVER (ORDER BY {column} DESC) AS Ranking
                    , {column}
                FROM {schema_and_table}
                WHERE {date_col} > '{date_filter}'
                  AND {column} IS NOT NULL
            )
            ,
            cte2 AS 
            (
                SELECT 1 AS row_order
                    , MIN({column}) as min_val
                FROM cte1 
                WHERE Ranking = {sample_size}
                UNION ALL
                SELECT 2
                    , MIN({column}) 
                FROM {schema_and_table}
            )

            SELECT min_val 
            FROM cte2
            WHERE min_val IS NOT NULL
            ORDER BY row_order ASC 
            LIMIT 1
        """

    @staticmethod
    def get_test_where_clause_full(schema_and_table, column, sample_size=0):
        """Returns the full where test clause 
        """

        return f"""
        with cte1 AS (
            SELECT RANK() OVER (ORDER BY {column} DESC) AS Ranking
                , {column}
            FROM {schema_and_table}
        )

        SELECT MAX({column})
        FROM cte1 WHERE Ranking = {sample_size}
        """

    @staticmethod
    def get_max_col_val(schema_and_table, column):
        return f"""
            SELECT MAX({column})
            FROM {schema_and_table}
        """

    @staticmethod
    def select_columns_from_table(
        schema_and_table,
        platform=Platform.VERTICA,
        extra_predicate="",
        data_type="all",
    ):
        """Returns query to get the columns of a table  
        
        Arguments:
            schema_and_table (str): The name of the table
            platform (Enum.Platform): The database system of the table 
            extra_predicate (str): An optional extra predicate 
            data_type (str): The date filter to help with the latenc of this query execution
        
        Returns: 
            query (str): The query to get the column names
        """
        schema, table = tuple(schema_and_table.split("."))

        information_schema = [Platform.SNOWFLAKE, Platform.YELLOWBRICK]
        v_catalog = [Platform.VERTICA]

        meta_schema = "INFORMATION_SCHEMA"
        if platform in v_catalog:
            meta_schema = "V_CATALOG"
        else:
            schema, table = schema.lower(), table.lower()

        return f"""
            SELECT COLUMN_NAME
            FROM {meta_schema}.COLUMNS
            WHERE TABLE_SCHEMA = '{schema}' 
              AND TABLE_NAME = '{table}'
              {extra_predicate}
        """

    # TODO: possibly simplify things by making StatementCatalog not accessible at all from outside of this file
    @staticmethod
    def vsql(query="", output_path="", field_delimiter=",", extra_output_args="", compressed=False,):
        """Builds the statement to run for vsql to execute
        
        Arguments:
            query (str): The query for vertica
            output_path (str): The path of the file where the data will go. If no path, then returned to user. 
	    field_delimiter (str): The delimiter used to seperated fields
            extra_output_args (str): Optional extra arguments
            compressed (str): Whether to pipe the data to a compression utility (gzip) 
        
        Returns: 
            query (str): The statment to send for vsql execution
        """
        output_args = extra_output_args
        if output_path and not compressed:
            output_args = (
                 f"{extra_output_args} -o {output_path} -F $'{field_delimiter}' "
            )

	if output_path and compressed:
	    output_args = (
		f"{extra_output_args} -F $'{field_delimiter}' | gzip -c > {output_path}"
	    )
	
        if query:
            query = f"""-c "{query}" """

        return f"""vsql -P footer=off -A {query} {output_args}"""

    @staticmethod
    def ybsql(query, output_path="", field_delimiter=",", extra_output_args=""):
        output_args = extra_output_args
        if output_path:
            output_args = (
                extra_output_args + f""" -o {output_path} -F "{field_delimiter}" """
            )

        return f"""ybsql -c "{query}" -A -t {output_args}"""

    @staticmethod
    def ybload(
        table,
        input_path,
        extras,
        field_delimiter=",",
        null_marker="",
        num_cores=os.cpu_count(),
    ):
	if ConstantCatalog.YB_NUM_CORES == 0:
		num_cores = os.cpu_count()
		
        return (
            f"""ybload -t {table} {input_path} --parse-header-line --nullmarker "{null_marker}" --max-bad-rows 1000 """
            + f"--num-cores {num_cores} --read-sources-concurrently ALLOW --num-readers {ConstantCatalog.YB_NUM_READERS} "
            + f"""--on-zero-char REMOVE --num-header-lines 1 --delimiter "{field_delimiter}" {extras} """
        )

    @staticmethod
    def getenvs(root_project_dir):
        return f". {root_project_dir}/scripts/getenv.sh"


class Execution:
    """Class for holding static methods to execute and return results
    
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

    @classmethod
    def vsql(cls, query="", output_path="", field_delimiter=",", extra_output_args=""):
        """Executes a vsql query
        
        Arguments:
            query (str): The query for vertica
            output_path (str): The path of the file where the data will go. If no path, then returned to user. 
	    field_delimiter (str): The delimiter used to seperated fields
            extra_output_args (str): Optional extra arguments
            compressed (str): Whether to pipe the data to a compression utility (gzip) 
        
        Returns: 
            results (bytes): Either returns the results if no output_path is given or it returns nothing
        """
        vsql = StatementCatalog.vsql(
            query, output_path, field_delimiter, extra_output_args
        )

        Execution.get_logger().log.debug(f"Execute VSQL: {vsql}")
        return Execution._execute(vsql)

    @classmethod
    def vsql_get_chunk_size(cls, table):
	"""Executes the query to tget the chunk size that will be used in a later query
	   The chunk size is used to break up the data into smaller chunks. The chunk
	   size is basically a predetermined amount of space ( set by a config right now )
	   and that chunk size in mb is translated to a chunk size amount of rows here
	   
	Arguments:
	    table(str): The table to get the row chunk size about 
	    
	Returns:
	    reults (str): This shoudl be an integer ALWAYS but it is a string datatype because
	       it will make predicate building easier with strign concatenation
	"""
	
        cli_result = Execution.vsql(
            StatementCatalog.get_chunk_size(schema_and_table=table),
            extra_output_args=" -t ",
        )

        return cli_result.decode().strip("\n\t ")

    @classmethod
    def vsql_get_table_size_mb(cls, table):
        cli_result = Execution.vsql(
            StatementCatalog.get_table_size_mb(table=table), extra_output_args=" -t ",
        )

        return cli_result.decode().strip("\n\t ")

    @classmethod
    def vsql_get_chunk_where_clauses(cls, table, column, chunk_size, predicate=""):
        cli_result = Execution.vsql(
            StatementCatalog.get_chunk_where_clauses(
                schema_and_table=table,
                column=column,
                chunk_size=chunk_size,
                predicate=predicate,
            ),
            extra_output_args=" -t ",
        )

        return cli_result.decode().strip("\n\t ").split("\n")

    @classmethod
    def vsql_get_max_col_val(cls, table, column):
        cli_result = Execution.vsql(
            StatementCatalog.get_max_col_val(schema_and_table=table, column=column),
            extra_output_args=" -t ",
        )

        return cli_result.decode().strip("\n\t ")

    @classmethod
    def vsql_get_sample_date_filter(
        cls, schema_and_table, part_col, date_col, sample_size
    ):
        cli_result = Execution.vsql(
            StatementCatalog.get_max_col_val(schema_and_table=table, column=column),
            extra_output_args=" -t ",
        )

        return cli_result.decode().strip("\n\t ")

    @classmethod
    def vsql_get_sample_filter_val(cls, table, part_col, sample_size):

        sample_size = sample_size + 1

        cli_result = Execution.vsql(
            StatementCatalog.get_test_where_clause(
                schema_and_table=table,
                column=part_col,
                sample_size=sample_size,
                date_filter=ConstantCatalog.DATE_FILTER(table),
                date_col=ConstantCatalog.DATE_COL(table),
            ),
            extra_output_args=" -t ",
        )

        min_val = cli_result.decode().strip("\n\t ")

        return min_val

    @classmethod
    def ybsql_truncate_tables(cls,):
        cli_result = Execution.ybsql(StatementCatalog.truncate_tables(),)

        return cli_result.decode().strip("\n\t ")

    @classmethod
    def ybsql_delete_date_range(cls, table, predicate):
	"""Executes a delete query that will delete the records from the target before it is loaded
	
	Arguments:
	    table (str): The name to delete from
	    predicate (str): The where clause of the delete statement 
	
	Returns:
	    None
	"""
	Execution.ybsql(StatementCatalog.delete_date_range(table, predicate),)
	
    @classmethod
    def ybsql(cls, query, output_path="", field_delimiter=",", extra_output_args=""):
        ybsql = StatementCatalog.ybsql(
            query, output_path, field_delimiter, extra_output_args
        )

        Execution.get_logger().log.debug(f"Execute YBSQL: {ybsql}")
        return Execution._execute(ybsql)

    @classmethod
    def get_table_column_names(
        cls,
        schema_and_table,
        platform=Platform.VERTICA,
        extra_predicate="",
        data_type="all",
    ):
        """Executes a query and returns the names of all the columns of the specified table

        Arguments:
            schema_and_table (str): The name of the table
            platform (Enum.Platform): The database system of the table (optional - default is VERTICA)
            extra_predicate (str): An optional extra predicate 
            data_type (str): The date filter to help with the latenc of this query execution
            
        Returns:
            query (list[str]):  A list of column names 
        """

        sql = ""
        query_out = b""

        sql = StatementCatalog.select_columns_from_table(
            schema_and_table=schema_and_table,
            platform=platform,
            extra_predicate=extra_predicate,
            data_type=data_type,
        )

        if platform == Platform.VERTICA:
            query_out = Execution.vsql(
                query=sql, field_delimiter=",", extra_output_args=" -t "
            )

        elif platform == Platform.YELLOWBRICK:

            query_out = Execution.ybsql(
                query=sql, field_delimiter=",", extra_output_args=" -t "
            )

        return list(filter(lambda s: len(s) > 0, query_out.decode().split("\n"),))

    @classmethod
    def _execute(cls, cmd):
        output = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
        return output

    @classmethod
    def ybload(cls, table, input_path, extras="", field_delimiter=",", null_marker=""):
        """Executes ybload with the supplied parameters

        Arguments:
            table (str): The name of the table to load (required)
            input_path (str): The input path to load from (required)
            extras (str): Any extra commanld line arguments to add to the defaults 
            fiel_delimiter (str): An optional field_delimiter argument 
            null_marker (str): An optional null_marker argument
            
        Returns:
            The bytes encoded object returned from the cli 
        """
        ybload = StatementCatalog.ybload(
            table=table,
            input_path=input_path,
            extras=extras,
            field_delimiter=field_delimiter,
            null_marker=null_marker,
        )

        logger.log.debug(f"Execute YBLOAD: {ybload}")
        return Execution._execute(ybload)


class ScriptCatalog:
    """Class for holding static methods that return bash scripts that this migration application will utilize
    
    Args:
        None
	
    Arguments:
        None
    """

    @staticmethod
    def yb_checksum_script():
        return r"""#!/bin/bash
#
#
#  (c) 2018 Yellowbrick Data Corporation.
#  This script is provided free of charge by Yellowbrick Data Corporation as a convenience to its customers.
#  This script is provided "AS-IS" with no warranty whatsoever.
#  The customer accepts all risk in connection with the use of this script, and Yellowbrick Data Corporation shall have no liability whatsoever.
#
# rc: 2021-06-16 V1.
#

# This function only gets called for text columns (CHAR/VARCHAR)
#
chksum_strings () {

    TheSchema=$1
    TheTable=$2

    CHAR_COUNT=`ybsql -q -v ON_ERROR_STOP=true -A -t -X <<EOF
      select count(*) from information_schema.COLUMNS
                WHERE  
			table_name = lower('${TheTable}')
                       AND table_schema = '${TheSchema}'
                       AND udt_name in ('char', 'bpchar', 'varchar');
EOF`
   if [ $CHAR_COUNT -eq 0 ]; then
      rowsize_chars=0
      echo  " select ':   SUCCESS ${TheSchema}.${TheTable}: no char/varchar columns ' as string_checksum ; "
   else
       TMPFILE="/tmp/tmp.`date +%Y%m%d%H%M%S`.$$"
       ybsql -q -v ON_ERROR_STOP=true -A -t -X <<EOF > $TMPFILE
       SELECT         -- table_schema, table_name,
                       CASE
                         WHEN ordinal_position = 1 THEN ' select  round(0 '
                         ELSE ' '
                       END
			||
			CASE
			WHEN udt_name = 'char' THEN '+ round(avg(length(coalesce("' || column_name || '"'
			WHEN udt_name = 'bpchar' THEN '+ round(avg(length(coalesce("' || column_name || '"'
			WHEN udt_name = 'varchar' THEN '+ round(avg(length(coalesce("' || column_name || '"'
                        ELSE
				' '
			END
                       || ' '
                       || CASE Coalesce(character_maximum_length, 0)
                            WHEN 0 THEN ''
                            ELSE ',''''))),2) -- ('
                                 || character_maximum_length
                                 || ')'
                          END
			-- , ordinal_position
                FROM   information_schema.COLUMNS
                WHERE  table_name = lower('${TheTable}')
                       AND table_schema = '${TheSchema}'
                       AND (udt_name in ('char', 'bpchar', 'varchar') or (ordinal_position=1))
order by ordinal_position
EOF
echo ",2) as string_checksum, count(*) as row_count from ${TheSchema}.${TheTable}  $WHERE_CLAUSE " >> $TMPFILE
    RESULTS=`ybsql -q -v ON_ERROR_STOP=true -A -t -X < $TMPFILE`
    rowsize_chars=`echo $RESULTS | cut -d'|' -f1`
    ROW_COUNT=`echo $RESULTS | cut -d'|' -f2`
        echo " " 
        echo "with string_sum as ( " 
        cat $TMPFILE
        rm $TMPFILE > /dev/null 2>&1
        echo ") " 
        echo  " select " 
    if [ $rowsize_chars > 0 ]; then
        echo -n "      case when string_checksum =  " 
        printf "%s " $rowsize_chars
        echo -n " and  row_count = " 
        printf "%s" $ROW_COUNT
        echo  " then ':   SUCCESS ' else ':  FAILURE: ' END  || "
        echo  " '${TheSchema}.${TheTable}: ' || coalesce(string_checksum,0) || ' vs $rowsize_chars(yb) and ROWS$SQLCASE_WHERE_CLAUSE: ' || row_count  || ' vs $ROW_COUNT(yb) ' as string_checksum"
        echo  " from string_sum; "
    else
        echo -n "      case when row_count = " 
        printf "%s" $ROW_COUNT
        echo  " then ':   SUCCESS ' else ':   FAILURE ' END  || "
        echo  "       '${TheSchema}.${TheTable} ROWS$SQLCASE_WHERE_CLAUSE: ' || row_count  || ' vs $ROW_COUNT(yb) ' as string_checksum "
        echo  " from string_sum; "
    fi
   fi
}
chksum_ints_numerics () {
    TheSchema=$1
    TheTable=$2
    CHAR_COUNT=`ybsql -q -v ON_ERROR_STOP=true -A -t -X <<EOF
      select count(*) from information_schema.COLUMNS
                WHERE  
			table_name = lower('${TheTable}')
                       AND table_schema = '${TheSchema}'
                       AND udt_name in ('int2', 'int4', 'int8', 'numeric', 'dec', 'decimal' );
EOF`
   if [ $CHAR_COUNT -eq 0 ]; then
      rowsize_chars=0
      echo  " select ':   SUCCESS ${TheSchema}.${TheTable}: no int/numeric/decimal columns ' as number_checksum ; "
   else
       TMPFILE="/tmp/tmp.`date +%Y%m%d%H%M%S`.$$"
       ybsql -q -v ON_ERROR_STOP=true -A -t -X <<EOF > $TMPFILE
       SELECT         -- table_schema, table_name,
                       CASE
                         WHEN ordinal_position = 1 THEN ' select  round(0 '
                         ELSE ' '
                       END
			||
			CASE
			WHEN udt_name in ('int2','int4','int8', 'numeric', 'dec', 'decimal') THEN '+ round(avg(coalesce("' || column_name || '",0)),2) -- ' || udt_name 
                        ELSE
				' '
			END
                       || ' '
			-- , ordinal_position
                FROM   information_schema.COLUMNS
                WHERE  table_name = lower('${TheTable}')
                       AND table_schema = '${TheSchema}'
                       AND (udt_name in ('int2', 'int4','int8', 'numeric', 'dec', 'decimal') or (ordinal_position=1))
order by ordinal_position
EOF
echo ",2) as number_checksum, count(*) as row_count from ${TheSchema}.${TheTable}  $WHERE_CLAUSE " >> $TMPFILE
    RESULTS=`ybsql -q -v ON_ERROR_STOP=true -A -t -X < $TMPFILE`
    rowsize_chars=`echo $RESULTS | cut -d'|' -f1`
    ROW_COUNT=`echo $RESULTS | cut -d'|' -f2`
        echo " " 
        echo "with number_sum as ( " 
        cat $TMPFILE
        rm $TMPFILE > /dev/null 2>&1
        echo ") " 
        echo  " select " 
    if [ $rowsize_chars > 0 ]; then
        echo -n "      case when number_checksum =  " 
        printf "%s " $rowsize_chars
        echo -n " and  row_count = " 
        printf "%s" $ROW_COUNT
        echo  " then ':   SUCCESS ' else ':   FAILURE ' END  || "
        echo  "      '${TheSchema}.${TheTable}: ' || coalesce(number_checksum,0) || ' vs $rowsize_chars(yb) and ROWS$SQLCASE_WHERE_CLAUSE: ' || row_count  || ' vs $ROW_COUNT(yb) ' as number_checksum"
        echo  " from number_sum; "
    else
        echo -n "      case when row_count = " 
        printf "%s" $ROW_COUNT
        echo  " then ':   SUCCESS ' else ':   FAILURE ' END  || "
        echo  "       '${TheSchema}.${TheTable} ROWS$SQLCASE_WHERE_CLAUSE: ' || row_count  || ' vs $ROW_COUNT(yb) ' as number_checksum "
        echo  " from number_sum; "
    fi
   fi
} # end of chksum_ints_numerics
# These types are inexact types, meaning that some values are stored as approximations, 
#  such that storing and returning a specific value may result in slight discrepancies.
#
chksum_floats () {
    TheSchema=$1
    TheTable=$2
    CHAR_COUNT=`ybsql -q -v ON_ERROR_STOP=true -A -t -X <<EOF
      select count(*) from information_schema.COLUMNS
                WHERE  
			table_name = lower('${TheTable}')
                       AND table_schema = '${TheSchema}'
                       AND udt_name in ('float', 'float4', 'float8', 'double precision');
EOF`
   if [ $CHAR_COUNT -eq 0 ]; then
      rowsize_chars=0
      echo  " select ':    SUCCESS ${TheSchema}.${TheTable}: no float/double columns ' as float_checksum ; "
   else
       TMPFILE="/tmp/tmp.`date +%Y%m%d%H%M%S`.$$"
       ybsql -q -v ON_ERROR_STOP=true -A -t -X <<EOF > $TMPFILE
       SELECT         -- table_schema, table_name,
                       CASE
                         WHEN ordinal_position = 1 THEN ' select  round(0 '
                         ELSE ' '
                       END
			||
			CASE
			WHEN udt_name in ('float', 'float4', 'float8', 'double precision') THEN '+ round(avg(coalesce("' || column_name || '",0)),2) -- ' || udt_name 
                        ELSE
				' '
			END
                       || ' '
			-- , ordinal_position
                FROM   information_schema.COLUMNS
                WHERE  table_name = lower('${TheTable}')
                       AND table_schema = '${TheSchema}'
                       AND (udt_name in ('float', 'float4', 'float8', 'double precision') or (ordinal_position=1))
order by ordinal_position
EOF
echo ",2) as float_checksum, count(*) as row_count from ${TheSchema}.${TheTable}  $WHERE_CLAUSE " >> $TMPFILE
    RESULTS=`ybsql -q -v ON_ERROR_STOP=true -A -t -X < $TMPFILE`
    rowsize_chars=`echo $RESULTS | cut -d'|' -f1`
    ROW_COUNT=`echo $RESULTS | cut -d'|' -f2`
        echo " " 
        echo "with float_sum as ( " 
        cat $TMPFILE
        rm $TMPFILE > /dev/null 2>&1
        echo ") " 
        echo  " select " 
    if [ $rowsize_chars > 0 ]; then
        echo -n "      case when float_checksum =  " 
        printf " %s " $rowsize_chars
        echo -n " and  row_count = " 
        printf "%s" $ROW_COUNT
        echo  " then ':    SUCCESS ' else ' FAILURE: ' END  || "
        echo  "      '${TheSchema}.${TheTable}:' || coalesce(float_checksum,0) || ' vs $rowsize_chars(yb) and ROWS$SQLCASE_WHERE_CLAUSE: ' || row_count  || ' vs $ROW_COUNT(yb) ' as float_checksum  "
        echo  " from float_sum; "
    else
        echo -n "      case when row_count = " 
        printf "%s" $ROW_COUNT
        echo  " then ':    SUCCESS ' else ':    FAILURE ' END  || "
        echo  "       '${TheSchema}.${TheTable}: ROWS$SQLCASE_WHERE_CLAUSE: ' || row_count  || ' vs $ROW_COUNT(yb) ' as float_checksum "
        echo  " from float_sum; "
    fi
   fi
} # end of chksum_floats
chksum_temporal () {
    TheSchema=$1
    TheTable=$2
    CHAR_COUNT=`ybsql -q -v ON_ERROR_STOP=true -A -t -X <<EOF
      select count(*) from information_schema.COLUMNS
                WHERE  
			table_name = lower('${TheTable}')
                       AND table_schema = '${TheSchema}'
                       AND udt_name in ('timestamp', 'timestamptz', 'time', 'date' );
EOF`
   if [ $CHAR_COUNT -eq 0 ]; then
      rowsize_chars=0
      echo  " select ': SUCCESS ${TheSchema}.${TheTable}: no date/ts/time columns ' as temporal_checksum ; "
   else
       TMPFILE="/tmp/tmp.`date +%Y%m%d%H%M%S`.$$"
       ybsql -q -v ON_ERROR_STOP=true -A -t -X <<EOF > $TMPFILE
       SELECT         -- table_schema, table_name,
                       CASE
                         WHEN ordinal_position = 1 THEN ' select  0 '
                         ELSE ' '
                       END
			||
			CASE
			WHEN udt_name in ('timestamp', 'timestamptz', 'time', 'date') THEN '+ count( distinct "' || column_name || '") -- ' || udt_name 
                        ELSE
				' '
			END
                       || ' '
			-- , ordinal_position
                FROM   information_schema.COLUMNS
                WHERE  table_name = lower('${TheTable}')
                       AND table_schema = '${TheSchema}'
                       AND (udt_name in ('timestamp', 'timestamptz', 'time', 'date') or (ordinal_position=1))
order by ordinal_position
EOF
echo " as temporal_checksum, count(*) as row_count from ${TheSchema}.${TheTable} $WHERE_CLAUSE " >> $TMPFILE
    RESULTS=`ybsql -q -v ON_ERROR_STOP=true -A -t -X < $TMPFILE`
    rowsize_chars=`echo $RESULTS | cut -d'|' -f1`
    ROW_COUNT=`echo $RESULTS | cut -d'|' -f2`
    if [ $rowsize_chars > 0 ]; then
        echo " " 
        echo "with temporal_sum as ( " 
        cat $TMPFILE
        rm $TMPFILE > /dev/null 2>&1
        echo ") " 
        echo  " select " 
        echo -n "      case when temporal_checksum =  " 
        printf "%s " $rowsize_chars
        echo -n " and  row_count = " 
        printf "%s" $ROW_COUNT
        echo  " then ': SUCCESS ' else ': FAILURE ' END  || "
        echo  "      '${TheSchema}.${TheTable}: ' || coalesce(temporal_checksum,0) || ' vs $rowsize_chars(yb) and ROWS$SQLCASE_WHERE_CLAUSE: ' || row_count  || ' vs $ROW_COUNT(yb) ' as temporal_checksum  "
        echo  " from temporal_sum; "
    fi
   fi
} # end of chksum_temporal
#
# Main
#
if [ ${#} -eq 3 ] || [ ${#} -eq 4 ] 
then
	echo
else
        echo "Usage $0 {databasename} {schema} {tablename|all} {optional where clause}"
        echo "  This script will generate string checksum SQL and expected results DB ${YBDATABASE} SCHEMA"
        echo "  Run as a usr who has read access to the tables and information_schema.COLUMNS "
        echo
        echo "  Yellowbrick Data Types Checked:"
        echo "  	string_checksum: CHAR, CHAR(n), VARCHAR(n)"
        echo "  	number_checksum: SMALLINT, INTEGER, BIGINT, NUMERIC(s,p), DEC(s,p), DECIMAL(s,p),"
        echo "  	temporal_checksum: DATE, TIMESTAMP, TIMESTAMP WITH TIME ZONE,TIMESTAMPZ, TIME"
        echo "  	float_checksum: REAL, DOUBLE PRECISION, FLOAT, FLOAT4, FLOAT8"
        echo "  	float_checksum: REAL, DOUBLE PRECISION, FLOAT, FLOAT4, FLOAT8"
        echo
        echo "  Yellowbrick Data Types Not Checked will produce a warning:"
        echo "  	WARNING: schema.tablename WARNING  1 cols/data types not checksumed"
        echo
        echo "  List Databases: "
        echo "     ybsql -q -A -t -X -F \" \" -c \"\l\" | egrep \"UTF8|LATIN9\"  | awk '{ print \$1 }'"
        echo
        echo "  List schemas: "
        echo "     ybsql -c '\dn'" 
        echo  
        echo "  Example: All tables in one schema "
        echo "    $ ./yb_checksum.sh rc_training sf10000 "all" > x 2>&1 "
        echo "    $ ybsql -A -t -x -z < x "
        echo "      string_checksum:   SUCCESS sf10000.call_center: 237.96 vs 237.96(yb) and ROWS: 54 vs 54(yb)"
        echo "      number_checksum:   SUCCESS sf10000.call_center: 318115702.95 vs 318115702.95(yb) and ROWS: 54 vs 54(yb)"
        echo "      temporal_checksum: SUCCESS sf10000.call_center: 7 vs 7(yb) and ROWS: 54 vs 54(yb)"
        echo "      float_checksum:    SUCCESS sf10000.call_center: no float/double columns"
        echo "      string_checksum:   SUCCESS sf10000.store_returns_bc: no char/varchar columns"
        echo "      number_checksum:   SUCCESS sf10000.store_returns_bc ROWS: 0 vs 0(yb)"
        echo "      temporal_checksum: SUCCESS sf10000.store_returns_bc: no date/ts/time columns"
        echo "      float_checksum:    SUCCESS sf10000.store_returns_bc: no float/double columns"
	echo "      number_checksum:   FAILURE public.foo_int: 150.00 vs 100.00(yb) and ROWS: 2 vs 1(yb)"
        echo "    "
        echo "  Example: one table, 2 tables, or all in a schema"
        echo "    $ ./yb_checksum.sh  rc_training sf10000 store > x"
        echo "    $ ./yb_checksum.sh  rc_training sf10000 'store store_returns' > x"
        echo "    $ ./yb_checksum.sh  rc_training sf10000 'all' > x"
        echo "    "
        echo "  Example: add a where clause on all tables; like a date "
        echo "    $ ./yb_checksum.sh rc_training public date_dim \"where d_date='2001-01-01'\" > x"
	echo "      string_checksum:   SUCCESS public.date_dim: 36.00 vs 36.00(yb) and ROWS  where d_date='2001-01-01'  : 1 vs 1(yb)"
        echo "    "
        echo "  Example: Comparison DB might have a different table name than YB's; use sed "
        echo "    $ ./yb_checksum.sh rc_training sf10000 store | sed -e 's/sf10000.store/sf10000_iq.store_iq/g' > x"
        echo "    "
	echo "    $ ybsql -A -t -x -z < x"
        echo "    "
        echo "Version: v2020_06_16 "
        echo "    "
        echo
        exit
fi
DATABASE=`echo $1 `
PARAM_2=`echo $2 `
PARAM_3=`echo $3 `
if [ ${#} -eq 4 ] 
then 
    WHERE_CLAUSE=`echo " $4 "`
    SQLCASE_WHERE_CLAUSE=`echo " $WHERE_CLAUSE " | sed -e "s/'/''/g"`
else
    WHERE_CLAUSE=""
    SQLCASE_WHERE_CLAUSE=""
fi
unset  PGDATABASE
export YBDATABASE=$DATABASE
# User could have PG and YB variables set.  This script will use YB for the database; since its passed in
#      host, user credentials are outside of this script.
#
#
# Get the list of tables
for SCHEMA in "${PARAM_2}"
do
   SEARCH=`echo "--search_path=${SCHEMA}"`
   export PGOPTIONS=$SEARCH
   # PGOPTIONS is a nice way to set teh schema globally withint this script.
   #
   if [ "$PARAM_3" = "all" ]
   then
        TABLES=`ybsql -q -A -t -X -F " " -c "\dt" > /dev/null 2>&1`
        # verify that we can connect to Yellowbrick
        # 
        if [ $? -ne 0 ]; then
             echo "ybsql ERROR: Can't connect to Yellowbrick via ybsql, Check YBHOST, YBUSER, and YBPASSWORD and run ybsql."
             # if we can't get an actual row count, then we need to exit, because all calcs are based on row counts. 
             #
             exit
        fi
        # a way to get a list of the tables in one command.
        #
        TABLES=`ybsql -q -A -t -X -F " " -c "\dt" | awk '{ print $2 }'`
   else
        TABLES=$PARAM_3
   fi
echo "-- Database: $YBDATABASE"  
for TABLE in $TABLES
do
echo "------------- ${SCHEMA}.${TABLE} ----------------------------------------------------------------------------------------------"
    THE_COUNT=`ybsql -q -v ON_ERROR_STOP=true -A -t -X -c "select count(*) from  ${SCHEMA}.${TABLE};"`
    if [ $? -ne 0 ]; then
        echo "$0 failed...Running select count(*) for ${SCHEMA}.${TABLE} command."
        # if we can't get an actual row count, then we need to exit, because all calcs are based on row counts. 
        #
        exit
    fi
    THE_ROWSIZE_AVG=0
    THE_UNCOMPRESSED_SIZE=0
    # Initialize 2 vars used in let statements.
    #
    # Get the avg(length(col)) of each char/varchar column 
    chksum_strings "${SCHEMA}" "${TABLE}"
    # Get the avg((col)) of each int*/numeric column 
    chksum_ints_numerics  "${SCHEMA}" "${TABLE}"
    # Get the count distinct((col)) of each date, timestamp, time  column 
    chksum_temporal  "${SCHEMA}" "${TABLE}"
    # Get the avg((col)) of each floating points, beware of floats, taht's why its separated
    chksum_floats  "${SCHEMA}" "${TABLE}"
    UNCHECK_COL_COUNT=`ybsql -q -v ON_ERROR_STOP=true -A -t -X <<EOF
      select count(*) from information_schema.COLUMNS
                WHERE  
		table_name = lower('${TheTable}')
                AND table_schema = '${TheSchema}'
                AND udt_name NOT  in ('char', 'bpchar', 'varchar')
                AND udt_name NOT  in ('int2', 'int4', 'int8', 'numeric', 'dec', 'decimal' )
                AND udt_name NOT  in ('timestamp', 'timestamptz', 'time', 'date' )
                AND udt_name NOT in ('float', 'float4', 'float8', 'double precision')
EOF`
   if [ $UNCHECK_COL_COUNT -gt 0 ]; then
      rowsize_chars=0
      echo  " select ' WARNING  $UNCHECK_COL_COUNT cols/data types not checksumed ' as \"WARNING: ${TheSchema}.${TheTable}\" ; "
   fi
    #
echo "-----------------------------------------------------------------------------------------------------------------------------------------"
echo
done # end of table loop
done # end of schema loop

"""

    @staticmethod
    def getenvs_script(env_file_path):
        return f"""
# This script is intended to be `source`d (e.g. `source getenv.sh`)
# and it will parse the .env file and export the variables defined within it
# TODO - handle multi-line comments, blank lines :-)

process_line(){{
  line=$1
  if [[ $line == "" || ! $line ]]; then
    return
  fi

  line_start=${{line:0:1}} # Test if the line starts with a comment and skip
  if [[ $line_start == "#" ]]; then
    return
  fi

  NAME=$(echo "$line" | IFS= awk '{{split($1,parts, "="); print parts[1]}}')
  VALUE=$(echo "$line" | IFS= awk -F\\" '{{split($1,parts, "="); print parts[2]}}')
  export $NAME="$VALUE"
  echo ${{NAME}}"||"${{VALUE}}
}}

while IFS= read line; do # `IFS= ` removes the default space separator
  process_line "$line"
done < {str(env_file_path)}/.env
process_line "$line" # handle a last line without a newline char
 """
