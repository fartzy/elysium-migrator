#!/bin/bash
out_dir=$1

print_usage(){
	echo "This script must be run with a table name, make sure the table name is case sensitive for the vertica."
	echo "\nUsage: $0 </path/to/output/dir/schemaname.tablename> \n"
}

if [ "${#out_dir}" -lt "2" ];
then 
	print_usage
	exit
fi
 

out_files="${out_dir}/*.csv"
total=0
for f in $out_files
do 
	echo "Summing lines in ${f}..."
	lines=$(wc -l $f | awk '{sum+=$1;} END {print sum;}')
	total=$(echo $total + $lines | bc)
done

echo "Total lines: ${total}"
