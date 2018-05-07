# -*- coding: utf-8 -*-
"""
Created on Tue May  1 18:14:06 2018

@author: srilatha.bandari
"""

import psycopg2
from hdfs import InsecureClient
#import subprocess
import paramiko
import datetime

def increment_load(tables,cur):
    for table in tables:
        tableName=table
        ts = datetime.datetime.now().strftime('_%Y%m%d_%H%M%S')
        query="COPY (SELECT * FROM "+tableName+" where LastModifiedDate>(select run_time from control_table where table_name='"+tableName+"')) TO '/tmp/"+tableName+"_CDC"+ts+".csv'"
        cur.execute(query)
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect('172.16.6.89', username='root', password='Welcome@1234')
        ftp = ssh.open_sftp()
        ftp.get("/tmp/"+tableName+"_CDC"+ts+".csv", "Gp/"+tableName+"_CDC"+ts+".csv")
        ftp.close()  
    #Connect To hadoop
        client = InsecureClient('http://172.16.4.144:50070',user='root')
        client.makedirs("/user/root/greenplum/source/"+tableName+"__ct","0777")
        client.upload("/user/root/greenplum/source/"+tableName+"__ct/","F:/Srilatha/Attunity-POC/Greenplum/Gp/"+tableName+"_CDC"+ts+".csv")

def full_load(tables,cur):
    for table in tables:
        tableName=table
        ts = datetime.datetime.now().strftime('_%Y%m%d_%H%M%S')
        query="COPY (SELECT * FROM "+tableName+") TO '/tmp/"+tableName+"_FL"+ts+".csv'"
        cur.execute(query)    
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect('172.16.6.89', username='root', password='Welcome@1234')
        ftp = ssh.open_sftp()
        ftp.get("/tmp/"+tableName+"_FL"+ts+".csv", "Gp/"+tableName+"_FL"+ts+".csv")
        ftp.close()      
        #Connect To hadoop
        client = InsecureClient('http://172.16.4.144:50070',user='root')
        client.delete("/user/root/greenplum/source/"+tableName,True)
        client.makedirs("/user/root/greenplum/source/"+tableName,"0777")
        client.upload("/user/root/greenplum/source/"+tableName+"/","F:/Srilatha/Attunity-POC/Greenplum/Gp/"+tableName+"_FL"+ts+".csv")
        sql="INSERT INTO control_table(table_name) VALUES(%s);"
        cur.execute(sql, (tableName,))
        connection.commit()

tables_list=['dim_patient','dim_doctor','dim_service','dim_diagnosis','dim_hospital','fact_invoice']
#Connect to Greenplum
connection = psycopg2.connect(host="172.16.6.89",database="swastha_dw", user="gpadmin", password="Welcome@1234")
cursor = connection.cursor()
#full_load(tables_list,cursor)
increment_load(tables_list,cursor)
connection.close()




