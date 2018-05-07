# -*- coding: utf-8 -*-
"""
Created on Thu May  3 11:16:08 2018

@author: srilatha.bandari
"""

import psycopg2

tables_list=['dim_patient','dim_doctor','dim_service','dim_diagnosis','dim_hospital','fact_invoice']
connection = psycopg2.connect(host="172.16.6.89",database="swastha_dw", user="gpadmin", password="Welcome@1234")
cursor = connection.cursor()
for table in tables_list:
    query="SELECT distinct\
    c.column_name, c.data_type,1 as pk\
    FROM\
    information_schema.table_constraints tc \
    JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name) \
    JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema AND tc.table_name = c.table_name AND ccu.column_name = c.column_name\
    where tc.constraint_type = 'PRIMARY KEY' and \
    tc.table_name = '"+table+"'\
    union\
    SELECT distinct\
    c.column_name, c.data_type,0 as pk\
    FROM\
    information_schema.table_constraints tc \
    JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema AND tc.table_name = c.table_name\
    where\
    tc.table_name = '"+table+"' and c.column_name not in (SELECT c.column_name\
    FROM\
    information_schema.table_constraints tc \
    JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name) \
    JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema AND tc.table_name = c.table_name AND ccu.column_name = c.column_name\
    where tc.constraint_type = 'PRIMARY KEY' and \
    tc.table_name = '"+table+"')"
    cursor.execute(query)
    row = cursor.fetchone()
    while row is not None:
        md_file = open("F:/Srilatha/Attunity-POC/Greenplum/Metadata/"+table+".csv","a")
        md_file.write(row[0]+","+row[1]+","+str(row[2])+"\n")
        md_file.close()
        row = cursor.fetchone()
    print(table+" file created")
cursor.close()
connection.close()