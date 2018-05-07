#!/bin/bash

bash -c '/usr/hdp/2.6.1.0-129/hive/bin/hive -f <(hdfs dfs -cat /user/root/attunity_poc/scripts/Reservation/Reservation_FL.hive)'

tbl_counts=$(hive -S -e "select count(*) from ss_target.Reservation_final")
scrpt="insert into table ss_target.ChangeTracker values('Reservation',$tbl_counts)"
hive -e "$scrpt;"

hdfs dfs -mkdir /user/root/attunity_poc/archive/Reservation
hdfs dfs -chmod 777 /user/root/attunity_poc/archive/Reservation
