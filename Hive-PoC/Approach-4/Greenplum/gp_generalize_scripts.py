# -*- coding: utf-8 -*-
"""
Created on Wed Apr 11 18:00:55 2018

@author: srilatha.bandari
"""

def create_dir(tbl):
    from hdfs import InsecureClient
    client = InsecureClient('http://172.16.4.144:50070')
    client.makedirs("/user/root/greenplum/scripts/"+tbl,"0777")

def move_files(tbl):
    from hdfs import InsecureClient
    client = InsecureClient('http://172.16.4.144:50070')
    client.upload("/user/root/greenplum/scripts/"+tbl,"/root/srilatha/attunity_poc/gp_scripts/"+tbl+"_FL.hive")
    client.upload("/user/root/greenplum/scripts/"+tbl,"/root/srilatha/attunity_poc/gp_scripts/"+tbl+"_IL.hive")
    client.upload("/user/root/greenplum/scripts/"+tbl,"/root/srilatha/attunity_poc/gp_scripts/"+tbl+"_FL.sh")
    client.upload("/user/root/greenplum/scripts/"+tbl,"/root/srilatha/attunity_poc/gp_scripts/"+tbl+"_IL.sh")

import os
dir_path = "/root/srilatha/attunity_poc/scripts/gp_SchemaFolder/"
for filename in os.listdir(dir_path):
        tbl_name = str(filename).split(".")[0]
        hive_fl_str = "drop table if exists gp_target."+tbl_name+"_final;\n"
        hive_fl_str = hive_fl_str+"create table gp_target."+tbl_name+"_final as\nselect * from gp_source."+tbl_name+";\n"
        
        #Writing full load to hive file
        hive_fl_file = open("/root/srilatha/attunity_poc/gp_scripts/"+tbl_name+"_FL.hive","w+")
        hive_fl_file.write(hive_fl_str)
        hive_fl_file.close()
        
        #Creating the incremental load script
        file_content=open(dir_path+"/"+filename,'r')
        line=file_content.readline()
        hive_il_str = "create table gp_target."+tbl_name+"_stg as\nselect "
        col_arr = []
        pk_arr = []
        ck_arr = []
        while(line!=""):
            col_arr.append(line.split(",")[0])
            pk_arr.append(line.split(",")[2].replace('\r','').replace('\n',''))
            line = file_content.readline() 
            columns = ""
        for col_index in range(len(col_arr)):
            if(pk_arr[col_index] == '1'):
                ck_arr.append(col_arr[col_index])
            else:
                columns=columns+col_arr[col_index]+","
        columns = columns[:-1]
        file_content.close()
        if(len(ck_arr)>1):
          pk=""
          for i in range(len(ck_arr)):
            pk=pk+ck_arr[i]+","
          pk = pk[:-1]
        elif(len(ck_arr)==1):
          pk=ck_arr[0]
        print(pk)
        hive_il_str = hive_il_str+pk+","+columns+" from\n(select *, ROW_NUMBER() OVER(Partition by "
        hive_il_str = hive_il_str+pk+" ORDER BY lastmodifieddate desc) As rnk from\n(select "+pk+",cast(date_format(lastmodifieddate, 'yyyy-MM-dd HH:mm:ss') as timestamp) as modified_date,"
        hive_il_str = hive_il_str+columns+" from gp_target."+tbl_name+"_final\nUNION ALL\nselect "+pk+",cast(date_format(lastmodifieddate, 'yyyy-MM-dd HH:mm:ss') as timestamp) as modified_date,"
        hive_il_str = hive_il_str+columns+" from gp_source."+tbl_name+"__ct) t2\n)A\nwhere Rnk=1;"
        hive_il_str = hive_il_str+"\n\n"+"alter table gp_target."+tbl_name+"_final rename to "+tbl_name+"_tmp;"
        hive_il_str = hive_il_str+"\n"+"alter table gp_target."+tbl_name+"_stg rename to gp_target."+tbl_name+"_final;"
        hive_il_str = hive_il_str+"\n"+"drop table "+tbl_name+"_tmp;\n"
        
        #Writing incremental load to hive file
        hive_il_file = open("/root/srilatha/attunity_poc/gp_scripts/"+tbl_name+"_IL.hive","w+")
        hive_il_file.write(hive_il_str)
        hive_il_file.close()
        
        scrpt_fl_str = "#!/bin/bash\n\n"
        scrpt_fl_str = scrpt_fl_str+"bash -c '/usr/hdp/2.6.1.0-129/hive/bin/hive -f <(hdfs dfs -cat /user/root/greenplum/scripts/"+tbl_name+"/"+tbl_name+"_FL.hive)'\n\n"
        scrpt_fl_str = scrpt_fl_str+"tbl_counts=$(hive -S -e \"select count(*) from gp_target."+tbl_name+"_final\")\nscrpt=\"insert into table gp_target.ChangeTracker values('"+tbl_name+"',$tbl_counts)\"\nhive -e \"$scrpt;\"\n\n"
        scrpt_fl_str = scrpt_fl_str+"hdfs dfs -mkdir /user/root/greenplum/archive/"+tbl_name
        scrpt_fl_str = scrpt_fl_str+"\n"+"hdfs dfs -chmod 777 /user/root/greenplum/archive/"+tbl_name+"\n"
        
        #Writing full load to shell script
        scrpt_fl_file = open("/root/srilatha/attunity_poc/gp_scripts/"+tbl_name+"_FL.sh","w+")
        scrpt_fl_file.write(scrpt_fl_str)
        scrpt_fl_file.close()
         
        scrpt_il_str = "#!/bin/bash\n\n"
        scrpt_il_str = scrpt_il_str+"isEmpty=$(hdfs dfs -count /user/root/greenplum/source/"+tbl_name+"__ct | awk '{print $2}')\nif [[ $isEmpty -eq 0 ]];then\n    echo 'Given Path is empty'\n else\n"
        scrpt_il_str = scrpt_il_str+"bash -c '/usr/hdp/2.6.1.0-129/hive/bin/hive -f <(hdfs dfs -cat /user/root/greenplum/scripts/"+tbl_name+"/"+tbl_name+"_IL.hive)'\n\n"
        scrpt_il_str = scrpt_il_str+"tbl_counts=$(hive -S -e \"select count(*) from gp_target."+tbl_name+"_final\")\nscrpt=\"update gp_target.ChangeTracker set count=\"$tbl_counts\" where tablename='"+tbl_name+"'\"\nhive -e \"$scrpt;\"\n\n"
        scrpt_il_str = scrpt_il_str+"today=`date +'%s'`\nhdfs dfs -ls /user/root/greenplum/source/"+tbl_name+"__ct | grep '^-' | while read line ; do\ndir_date=$(echo ${line} | awk '{print $6}')\ndifference=$(( ( ${today} - $(date -d ${dir_date} +%s) ) / ( 24*60*60 ) ))\nfilePath=$(echo ${line} | awk '{print $8}')\nfilename=${filePath%.*}\nif [ ${difference} -ge 0 ]; then\ncurrent_ts=`date '+%Y%m%d_%H%M%S'`\nfilename=${filename}_${current_ts}.csv\nhdfs dfs -mv $filePath $filename\nstatus=`hdfs dfs -mv ${filename} /user/root/greenplum/archive/"+tbl_name+"/`\nfi\ndone\nfi\n\n"
        
        #Writing incremental load to shell script
        scrpt_il_file = open("/root/srilatha/attunity_poc/gp_scripts/"+tbl_name+"_IL.sh","w+")
        scrpt_il_file.write(scrpt_il_str)
        scrpt_il_file.close()
        
        #Creating the directories and moving the script files
        create_dir(tbl_name)
        move_files(tbl_name)

    

