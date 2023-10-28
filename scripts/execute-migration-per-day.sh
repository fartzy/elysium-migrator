mindate=$1
runteimdate=$2
# For simplicity, we just forcing scriptlong directory and applogsdir to the same directory.
# So both are coming from arg 2
scriptlog=$2
applogsdir=$2

applogname=$3
maxdate=$4
datadir=$5
validationdir=$6

finish(){
    dt=`date '+%Y-%m-%d %H:%M:%S'`
    echo "["${dt}"] The last day loaded was "${lastdate}". Check the logs to see if import for the next day already started." >> ${scriptlog}
    rm -rf ${datadir}/*
}
trap finish EXIT

print_usage(){
      echo -e "\nThis script executes the cli application 'migrate-cli' with a one-day window over a period of N days."
      echo "Execute this script from the root directory of the source code."
      echo -e "\nUsage: . ./execute-migration-per-day <>mindate <logdir> <applog> <maxdate> <datadir> <validationdir>"
      echo -e "\nArguments:"
      echo -e "\n mindate - The day of the first day to start."
      echo -e "\n logdir - The directory to store the archived logs of the application."
      echo -e "\n applog - The name of the log that the application generates. Call ti something specific so it doesn't "
      echo "             conflict with other instances."
      echo -e "\n maxdate - This is the max date that script will be running until."
      echo -e "\n datadir - Thisis where the data will be exported to and imported from."
      echo -e "\n validationdir- This directory is where the results of the validation will be stored at.\n\n"
}

if [ "$#" -ne "6" ];
then 
       print_usage
       exit 1
fi 

scriptlog=${scriptlog}"/migration_script_to_"${maxdate}".LOG"

init(){
    
    if [ ! -d $datadir ]; then 
        mkdir -p $datadir
    fi 
    
    if [ ! -d ${scriptlogdir} ]; then 
        mkdir -p ${scriptlogdir}
    fi 
    
    if [ -f ${scriptlog} ]; then 
        dt=`date '+%Y%m%d%H%M%S'`
        mv $scriptlog ${scriptlog}"/migration_script_to_"${maxdate}"_"${dt}".LOG"
    fi 
    
        
    if [ -f ${applogsdir}"/logs" ]; then 
        mkdir -p ${applogsdir}"/logs"
    fi 
    
    if [ -f ${validationdir} ]; then 
        mkdir -p ${validationdir}
    fi 
    
    #Ephemeral storage anyway
    chmod 777 ${applogsdir}/logs
    chmod 777 ${scriptlogdir}
    chmod 777 ${datadir}
    chmod 777 ${validationdir}
   
init 

dt=`date '+%Y-%m-%d %H:%M:%S'`

echo -e "\n["$dt"] All application logs for this run are being generated to "${applogsdir}"/logs." | tee -a ${scriptlog}
let daysbackstart=($(date +%s -d $runtimedate)-$(date +%s -d $mindate))/86400
arr=$(eval echo {1..$daysbackstart..1})
lastdate=""
for day in $arr; do 
    
    daysback=`expr $daysbackstart - $day`
    daysbackplusone=`expr $daysbackstart - $day + 1`
    
    todate=`date --date=="$runtime $daysback day ago" +%Y-%m-%d`
    fromdate=`date --date=="$runtime $daysbackplusone day ago" +%Y-%m-%d`
    
    dt=`date '+%Y-%m-%d %H:%M:%S'`
    echo "["$dt"] Exporting data from ${fromdate} to ${todate}..." | tee -a ${scriptlog}
    
    migrate-cli export -o ${datadir} -c $(pwd)/configuration/config.yaml -ed $(pwd) -ll DEBUG -lp ${applogname} --from-date {fromdate}" 00:00:00" -td ${todate}" 00:00:00" 2>&1 1 >> ${scriptlog} 
    
    dt=`date '+%Y-%m-%d %H:%M:%S'`
    echo "["$dt"] Exporting finished. Now importing from ${fromdate} to ${todate}..." | tee -a ${scriptlog}   
    
    dt=`date '+%Y%m%d%H%M%S'`
    cp ${applogname} ${applogsdir}"/logs/migrat-export-"${fromdate}"-to-"${todate}"_"${dt}.log" 2>&1 1>>${scriptlog}
    
    migrate-cli import -o ${datadir} -c $(pwd)/configuration/config.yaml -ed $(pwd) -ll DEBUG -lp ${applogname} -vd {validationdir} 2>&1 1 >> ${scriptlog}

    dt=`date '+%Y-%m-%d %H:%M:%S'`
    echo "["$dt"] Import finished." | tee -a ${scriptlog}   
    
    dt=`date '+%Y%m%d%H%M%S'`
    cp ${applogname} ${applogsdir}"/logs/migrat-import-"${fromdate}"-to-"${todate}"_"${dt}.log" 2>&1 1>>${scriptlog}
    
    
    #To archive and compress the logs into gzip compressed files 
    willcompress=$(( $day % 20 ))
    if [ "willcompress" -eq "0" ]; then 
        dt=`date '+%Y%m%d%H%M%S'`
        tar -czvf ${applogsdir}/logs/migrate-import-logs-${dt}.tar.gz ${applogsdir}/logs/*import*.log
        find ${applogsdir}/logs -name "*import*.log" -type f mmin +1 -delete
        tar -czvf ${applogsdir}/logs/migrate-export-logs-${dt}.tar.gz ${applogsdir}/logs/*export*.log
        find ${applogsdir}/logs -name "*export*.log" -type f mmin +1 -delete
    fi
    
    lastdate=$fromdate
done;
