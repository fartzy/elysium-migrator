#!/bin/bash
mindate=$1
maxdate=$2
applogname=$3
scriptlogdir=$4
runtimedate=`date '+%Y-%m-%d'`
# For simplicity, we just forcing scriptlog directory as well as others to the same directory. 
# So all are coming from arg 4

applogsdir=${scriptlogdir}"/log"
datadir=${scriptlogdir}"/data"
validationdir=${scriptlogdir}"/validation"
lastdate=""
scriptlog=""

finish(){
	dt=`date '+%Y-%m-%d %H:%M:%S'`
	echo "["$dt"] The last day loaded was "${lastdate}". Check logs to see if import for the next day already started." >> "${scriptlog}"
	rm -rf ${datadir}/*
}
trap finish EXIT

print_usage(){
        echo -e "\nThis script executes the cli application 'elysium-migrate-cli' with a day long window."
		echo "Execute this script from the root directory of the source code." 
	echo -e "\nArgs passed: "$@
	echo -e "\nUsage: . ./execute-elysium-migration-per-day <mindate> <maxdate> <applog> <executiondir>"
	echo -e "\nArguments:"
	echo -e "\n	<mindate> - The day of the first day to start."
	echo -e "\n	<maxdate> - The max date that it needs to be running until."
	echo -e "\n	<applog> - The <applog> argument is the full file path of the current log actually being " 
	echo "			generated by the migration application itself."
    echo -e "\n	<executiondir> - The directory to output the log of the script. Also the root directory to the
					application logs, the validation and the data itself."

}

if [ "$#" -ne "4" ];
then
        print_usage
        exit 1
fi

scriptlog=${scriptlogdir}"/elysium_migration_script_to_"${maxdate}".LOG"

init(){

	if [ ! -d $datadir ]; then
		mkdir -p $datadir
	fi

	if [ ! -d ${scriptlogdir} ]; then
        	mkdir -p ${scriptlogdir} 
	fi

	if [ -f $scriptlog ]; then
        	dt=`date '+%Y%m%d%H%M%S'`
        	mv $scriptlog ${scriptlogdir}"/elysium_migration_script_to_"${maxdate}"_"${dt}".LOG"
	fi

	if [ ! -d ${applogsdir} ]; then 
		mkdir -p ${applogsdir}
	fi	

	if [ ! -d ${validationdir} ]; then 
		mkdir -p ${validationdir}
	fi	

	chmod 777 ${applogsdir}
	chmod 777 ${scriptlogdir}
	chmod 777 ${datadir}
	chmod 777 ${validationdir}
}

init

dt=`date '+%Y-%m-%d %H:%M:%S'`

echo -e "\n["$dt"] All application logs for this run are being generated to "${scriptlogdir}"..." | tee -a $scriptlog
let daysbackstart=($(date +%s -d $runtimedate)-$(date +%s -d $mindate))/86400
arr=$(eval echo {1..$daysbackstart..1})
lastdate=""
for day in $arr; do 

	daysback=`expr $daysbackstart - $day`
	daysbackplusone=`expr $daysbackstart - $day + 1` 
 	
	todate=`date --date="$runtimedate $daysback day ago" +%Y-%m-%d`
	fromdate=`date --date="$runtimedate $daysbackplusone day ago" +%Y-%m-%d`

	dt=`date '+%Y-%m-%d %H:%M:%S'`
	# break from loop if date is equal to max date 
	if [ "${fromdate}" = "${maxdate}" ]; then 
 		echo "["$dt"] Procesing date and last date are "${maxdate}"."
		finish 
		exit 0
	else 
		echo "["$dt"] Processing "${fromdate}". Last date "${maxdate}"."
	fi
	
	dt=`date '+%Y-%m-%d %H:%M:%S'`
	echo "["$dt"] Exporting data from ${fromdate} to ${todate}..." | tee -a $scriptlog
	sleep 2
	
	elysium-migrate-cli export -o ${datadir} -c $(pwd)/tests/configuration/vertica_test_all.yaml -ed $(pwd) -ll DEBUG  -lp ${applogname} --from-date ${fromdate}" 00:00:00" --to-date ${todate}" 00:00:00" 2>&1 1>>${scriptlog}
	
	dt=`date '+%Y-%m-%d %H:%M:%S'`
	echo "["$dt"] Export finished. Now importing data from ${fromdate} to ${todate}..." | tee -a $scriptlog
	
	dt=`date '+%Y%m%d%H%M%S'`
	cp ${applogname} ${applogsdir}"/elysium-migrate-export-"${fromdate}"-to-"${todate}"_"${dt}".log" 2>&1 1>>${scriptlog}
	
	elysium-migrate-cli import -o ${datadir} -c $(pwd)/tests/configuration/vertica_test_all.yaml -ed $(pwd) -ll DEBUG -lp ${applogname} -vd ${validationdir} 2>&1 1>>${scriptlog}
	

	dt=`date '+%Y-%m-%d %H:%M:%S'`
	echo "["$dt"] Import of data from ${fromdate} to ${todate} finished." >> ${scriptlog}

	dt=`date '+%Y%m%d%H%M%S'`
	cp ${applogname} ${applogsdir}"/elysium-migrate-import-"${fromdate}"-to-"${todate}"_"${dt}".log" 2>&1 1>>${scriptlog}


	#To archive and compress the logs into gzip compressed files
	willcompress=$(( $day % 20  ))
	if  [ "$willcompress" -eq "0" ]; then
		dt=`date '+%Y%m%d%H%M%S'` 
		tar -czvf ${applogsdir}/elysium-migrate-import-logs-${dt}.tar.gz ${applogsdir}/*import*.log
		find ${applogsdir} -name "*import*.log" -type f -mmin +1 -delete		
		tar -czvf ${applogsdir}/elysium-migrate-export-logs-${dt}.tar.gz ${applogsdir}/*export*.log 
		find ${applogsdir} -name "*export*.log" -type f -mmin +1 -delete	
	fi
	

	lastdate=$fromdate
done;