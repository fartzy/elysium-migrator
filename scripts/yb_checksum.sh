#!/bin/bash
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
