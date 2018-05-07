hr = "09"
mnt = "20"
import os
dir_path = "/root/srilatha/attunity_poc/scripts/SchemaFolder"
for filename in os.listdir(dir_path):
        tbl_name = str(filename).split(".")[0]
        filePath = "hdfs dfs -cat /user/root/attunity_poc/scripts/"+tbl_name+"/"+tbl_name+"_FL.sh | exec sh"
        command = mnt+" "+hr+" * * * "+filePath
        print(command)
