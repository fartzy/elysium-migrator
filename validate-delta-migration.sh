daysback=$1
daysback=$((daysback-1))
rootdir=$2
runtimedate=$3
failures=""
recipients="elysium-dev@finance.com,elysium-operations-low-urgency@finance-notify.pagerduty.com"

rundate=`date --date="$runtimedate $daysback day ago" +%Y-%m-%d`
valdir="${rootdir}/${rundate}/validation"
	
echo "validating ${rundate}..."

if [ ! -d $valdir ]; then
       	failures="$failures\nTarget validation directory $valdir does not exist."
else 
	failure=$(grep -r "FAILURE" ${rootdir}/${rundate}/validation)
	if [[ ${#failure} -gt 1 ]] ; then
	
		failures=$(printf "%s\n%s" "$failures" "$failure")
	fi
fi 
	

# The length to check for is just the length of failures plus 5.  There is no reason a real failure would not meet that length
l=${daysback}
l=$((l+3))

if [[ ${#failures} -gt ${l} ]] ; then 
/usr/sbin/sendmail "$recipients" <<EOF
subject:Elysium Delta Load Validation Failure

These validation failures were found:
$failures
EOF
fi 
