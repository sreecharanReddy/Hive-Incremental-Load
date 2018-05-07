#!/bin/bash

isEmpty=$(hdfs dfs -count /user/root/attunity_poc/source/Reservation__ct | awk '{print $2}')
if [[ $isEmpty -eq 0 ]];then
    echo 'Given Path is empty'
 else
bash -c '/usr/hdp/2.6.1.0-129/hive/bin/hive -f <(hdfs dfs -cat /user/root/attunity_poc/scripts/Reservation/Reservation_IL.hive)'

tbl_counts=$(hive -S -e "select count(*) from ss_target.Reservation_final")
scrpt="update ss_target.ChangeTracker set count="$tbl_counts" where tablename='Reservation'"
hive -e "$scrpt;"

today=`date +'%s'`
hdfs dfs -ls /user/root/attunity_poc/source/Reservation__ct | grep '^-' | while read line ; do
dir_date=$(echo ${line} | awk '{print $6}')
difference=$(( ( ${today} - $(date -d ${dir_date} +%s) ) / ( 24*60*60 ) ))
filePath=$(echo ${line} | awk '{print $8}')
filename=${filePath%.*}
if [ ${difference} -ge 0 ]; then
current_ts=`date '+%Y%m%d_%H%M%S'`
filename=${filename}_${current_ts}.csv
hdfs dfs -mv $filePath $filename
status=`hdfs dfs -mv ${filename} /user/root/attunity_poc/archive/Reservation/`
fi
done
fi

