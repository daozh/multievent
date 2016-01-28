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
PreEventMinute=1
PostEventMinute=5

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
	filepath=MDBFilePath+dt.strftime(r'%Y/%m/')
	filedt= dt.replace(hour = dt.hour / 6*6)
	filename=r'db1_'+filedt.strftime('%m%d_%Y_%H') +'*.mdb'
	datafile = glob.glob(filepath+filename)
	if not datafile:
		if dt.hour >= 1:
			filedt=filedt.replace(hour=((dt.hour-1) / 6*6)+1)
		else:
			filedt=filedt.replace(hour=19)-timedelta(days=1)
        filename=r'db1_'+filedt.strftime('%m%d_%Y_%H') +'*.mdb'
        datafile = glob.glob(filepath+filename)
        #print datafile
	#find if event data are across two files 
	if datafile: 
		filetime=datetime.strptime(datafile[0][-20:-4],'%m%d_%Y_%H%M%S')
		moredatafile=[]
		if dt-timedelta(minutes=PreEventMinute)<filetime:
			print dt, dt-timedelta(minutes=PreEventMinute),'<',filetime,
			filedt=filedt-timedelta(hours=6)
			filename=r'db1_'+filedt.strftime('%m%d_%Y_%H') +'*.mdb'
			moredatafile = glob.glob(filepath+filename)
		if dt+timedelta(minutes=PostEventMinute)>filetime+timedelta(hours=6):
			filedt=filedt+timedelta(hours=6)
			filename=r'db1_'+filedt.strftime('%m%d_%Y_%H') +'*.mdb'
			moredatafile = glob.glob(filepath+filename)
			print dt,dt+timedelta(minutes=PostEventMinute),'>',filetime
		datafile.extend(moredatafile)
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
eventlist= getEventList(EventListFile)
#print eventlist[1:10]
for event in eventlist:

	if event['dt'] > datetime (2015,12,1):
		mdbfiles = searchfiles(event['dt'])
		if  len(mdbfiles)>0:
			print event['grid'],event['dt'].strftime('%m%d_%Y_%H%M%S'),mdbfiles
		else:
			print event['grid'],event['dt'].strftime('%m%d_%Y_%H%M%S'),"Data File is NOT Found!!!!"
		eventfdr=str.strip(str(fdrlist[event['grid']]))
		eventdt = event['dt'].strftime('#%Y-%m-%d %H:%M:%S#')
		preEventDt=(event['dt']-timedelta(seconds=1)).strftime('#%Y-%m-%d %H:%M:%S#')
		postEventDt=(event['dt']-timedelta(minutes=0)).strftime('#%Y-%m-%d %H:%M:%S#')
		for mdb in mdbfiles:
			conn =pyodbc.connect(r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ='+mdb)
			cursor=conn.cursor()
			sql='SELECT `Sample_Date&Time`,ConvNum,FinalFreq FROM FRURawData1009 WHERE `Sample_Date&Time` >='+preEventDt+' AND `Sample_Date&Time` <'+postEventDt
			print sql
			for row in cursor.execute(sql):
				print row
			cursor.close()
			conn.close()
