# Import multiple event data data from MS Access MDB file 

__author__='Dao'

import pyodbc
import math
import glob
import csv
from datetime import datetime, timedelta

FDRListFile= 'fdrgrid.csv' #(FDR#,Grid)(620,EI\n)
EventListFile ='eventlist.csv' #(Grid, date, UTCtime)(EI,2011-12-05,041530)
MDBFilePath='G:/' #./Year/Month 2015


def median(l):
    half = len(l) // 2
    for ind,elm in enumerate(l):
        l.pop(ind)
        l.insert(ind,math.sqrt(math.sqrt(elm)))
    l.sort()
    if not len(l) % 2:
        return (l[half - 1] + l[half]) / 2.0
    return pow(l[half],4)

def searchfiles(dt):
	datafile=''
	if dt < datetime (2015,1,1):
		return datafile
	filepath=MDBFilePath+dt.strftime('%Y/%m/')
	filedt= dt.replace(hour = dt.hour / 6*6)
	filename='db1_'+filedt.strftime('%m%d_%Y_%H') +'*.mdb'
	datafile = glob.glob(filepath+filename)
	if not datafile:
		if filedt.hour >= 1:
			filedt=filedt.replace(hour=filedt.hour+1)
		else:
			filedt=filedt.replace(hour=19)-timedelta(days=1)
        filename='db1_'+filedt.strftime('%m%d_%Y_%H') +'*.mdb'
        #print filename
        datafile = glob.glob(filepath+filename)
        #print datafile
	return datafile
def getEventList(eventfile):
	with open (eventfile) as evfile:
		eventlist=csv.reader(evfile)
		evlist=[]
		for row in eventlist:
			evinfo=dict()
			evinfo['grid']=row[0]
			evinfo['dt']=datetime.strptime(row[1]+row[2],'%Y-%m-%d%H%M%S')
			evlist.append(evinfo)
	return evlist

def getFDRList(fdrfile):
	with open (fdrfile,'r') as fdrf:
		fdrs=dict(EI=[],WI=[],TI=[],QI=[])
		fdrgrid=csv.reader(fdrf)
		for row in fdrgrid:
			fdrs[row[1].strip()].append(int(row[0]))
	return fdrs



fdrlist=getFDRList(FDRListFile)
#print fdrlist
eventlist= getEventList(EventListFile)
#print eventlist[1:10]
for event in eventlist:
	mdbfile = searchfiles(event['dt'])
	if mdbfile:
		print event['grid'],datetime.strftime(event['dt'], '%m%d_%Y_%H%M%S'), mdbfile

"""
dbfile=r'C:\Users\dzhou3\My Work\DataDump\Data\db1_0101_2015_080101.mdb'
conn =pyodbc.connect(r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ='+dbfile)
cursor=conn.cursor()

for row in cursor.execute('SELECT * FROM FRURawData1009 WHERE Index<100'):
    print row.FinalFreq


for row in cursor.tables():
    print row.table_name
print "\n"
cursor.close()
conn.close()
"""