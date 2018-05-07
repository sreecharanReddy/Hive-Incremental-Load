# -*- coding: utf-8 -*-
"""
Created on Thu Apr 12 16:22:18 2018

@author: Sreecharan.Akireddy
"""

import pymssql
import csv

conn = pymssql.connect(server='GGKU3SER2', user='sa', password='Welcome@1234', database='Swastha_QA_V02')
cursor = conn.cursor()

cursor.execute("""Select id,name from  
    sysobjects so where so.xtype='U'""")

tables=cursor.fetchall()
for table in tables:
    #print(table[0])
    cursor.execute("""SELECT
    c.name 'Column Name',
    t.Name 'Data type',
    CAST(ISNULL(i.is_primary_key, 0) AS varchar(2)) AS 'Primary Key'
FROM    
    sys.columns c
INNER JOIN 
    sys.types t ON c.user_type_id = t.user_type_id
LEFT OUTER JOIN 
    sys.index_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id and index_id=1
LEFT OUTER JOIN 
    sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id 
WHERE c.object_id = '"""+str(table[0])+"""' 
Order By 'Primary Key' DESC,'Column Name'""")
    columns=cursor.fetchall()
    if columns[0][2]=="0":
        continue
        
    g=open("C:\\Users\\sreecharan.akireddy\\Desktop\\Attinuity\\PythonCreatedMetaData\\"+table[1]+".csv","w", newline="")
    w=csv.writer(g)
    w.writerow(("FieldName","DataType","IsPrimary"))
    for column in columns:
        w.writerow((column[0],column[1],column[2]))
    g.close()
