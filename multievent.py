# Import multiple event data data from MS Access MDB file 

__author__='Dao'

import pyodbc
dbfile=r'C:\Users\dzhou3\My Work\DataDump\Data\db1_0101_2015_080101.mdb'
conn =pyodbc.connect(r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ='+dbfile)
cursor=conn.cursor()

for row in cursor.execute('SELECT * FROM FRURawData1009 WHERE Index<100'):
    print row.FirstFreq

"""
for row in cursor.tables():
    print row.table_name
print "\n"
"""
cursor.close()
conn.close()