# Import multiple event data data from MS Access MDB file 

__author__='Dao'

import pyodbc
import math
import glob
import csv
from datetime import datetime, timedelta

FDRListFile= 'fdrgrid.csv' #(FDR#,Grid)(620,EI\n)
EventListFile ='eventlist2016.csv' #(Grid, date, UTCtime)(EI,2011-12-05,041530)
MDBFilePath='G:/' #./Year/Month 2015
outputpath='./'
DateRange= [datetime(2016,3,3),datetime(2016,3,4)] 
PreEventMinute=1
PostEventMinute=5
HourMDBLength=6
RefUnit=732 # Reference Unit used to check the starting time in the mdb file
def median(l):
    l.sort()
    half = len(l) // 2
    if not len(l) % 2:
        return (l[half - 1] + l[half]) / 2.0
    return l[half]

def searchfiles(dt):
	datafile=''
	filepath=MDBFilePath+dt.strftime(r'%Y/%m/')
	monthfiles=glob.glob(filepath+"*.mdb")
	monthfiles.sort()
	monthfilename=[x[-20:-8] for x in monthfiles]
	filedt= dt.replace(hour = dt.hour // HourMDBLength*HourMDBLength)
	filename=filedt.strftime('%m%d_%Y_%H') 
	#print monthfilename
	#print monthfiles
	#print filename
	if filename in monthfilename:
		fileindex=monthfilename.index(filename)
	else:
		filedt=filedt+timedelta(hours=1)
		filename=filedt.strftime('%m%d_%Y_%H') 
		if  filename in monthfilename:
			fileindex=monthfilename.index(filename)
		else:
			return []
	#print fileindex
	if fileindex>0:
		datafile=monthfiles[fileindex-1:fileindex+2]
	else:
		datafile= monthfiles[fileindex:fileindex+2]
	return datafile
def getEventList(eventfile):
	with open (eventfile) as evfile:
		eventlist=csv.reader(evfile)
		evlist=[]
		for row in eventlist:
			evinfo=dict()
			evinfo['grid']=row[0]
			evinfo['dt']=datetime.strptime(row[1]+row[2],'%Y-%m-%d%H%M%S')
			eventdt = evinfo['dt'].strftime('_%Y%m%d_%H%M%S')
			outfile=evinfo['grid']+'/'+evinfo['grid']+eventdt+'.csv'
			if not glob.glob(outfile):  #if output file  not exist already 
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
errorlog=open('error.log','w+')

for event in eventlist:
	if event['dt'] < DateRange[0] or event['dt'] > DateRange[1]:
		continue
	print event
	mdbfiles = searchfiles(event['dt'])
	if  len(mdbfiles)>0:
		print event['grid'],event['dt'].strftime('%m%d_%Y_%H%M%S\n'),mdbfiles
	else:
		errorlog.write( event['grid']+','+event['dt'].strftime('%Y-%m-%d,%H%M%S')+"--Data File is NOT Found!!!!\n")
		continue
	eventfdr=fdrlist[event['grid']]
	#get event start and end time, shift 11 second
	preEventDt=(event['dt']-timedelta(seconds = PreEventMinute*60 +11)) 
	postEventDt=(event['dt']+timedelta(seconds =PostEventMinute * 60 -11))
	#print preEventDt,postEventDt
	freqdict=dict()
	for mdb in mdbfiles:
		print mdb
		conn =pyodbc.connect(r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ='+mdb)
		cursor=conn.cursor()
		#check if time in file is correct
		sql='SELECT `Sample_Date&Time` FROM FRURawData'+str(RefUnit)+' WHERE Index=1'
		row=cursor.execute(sql).fetchone()
		if row:
			filetime=row[0]
		else:
			continue
		if (filetime +timedelta (hours=HourMDBLength) )< preEventDt or filetime>postEventDt:
			#print "File time out of range"
			cursor.close()
			conn.close()
			continue
		print filetime
		fdrcount=0
		for fdr in eventfdr:
			if fdrcount>=20:
				break
			tablename='FRURawData'+str(fdr)
			if cursor.tables(table=tablename).fetchone():
				sql='SELECT `Sample_Date&Time`,ConvNum,FinalFreq FROM '+tablename+' WHERE `Sample_Date&Time` >='+preEventDt.strftime('#%Y-%m-%d %H:%M:%S#')+' AND `Sample_Date&Time` <'+postEventDt.strftime('#%Y-%m-%d %H:%M:%S#')
				#print fdr
				rows=cursor.execute(sql).fetchall()
				if len(rows)>0:
					print fdr,
					fdrcount+=1
					for row in rows:
						timeshift=float((row[0]-event['dt']+timedelta(seconds = PreEventMinute*60 +11)).seconds)+float(row[1])/10
						freq=round(row[2],4)
						if str(timeshift) in freqdict:
							freqdict[str(timeshift)]=freqdict[str(timeshift)]+[freq]
							#print tstamp,freqdict[str(tstamp)]
						else:
							freqdict[str(timeshift)]=[freq]
					#print freqdict	
		cursor.close()
		conn.close()
		if len(freqdict.keys()) >=3600:
			break
	timeshiftlist=[float(x) for x in freqdict.keys()]
	timeshiftlist.sort()
	print '\nData Length:',len(timeshiftlist)
	if len(timeshiftlist)<3600 and len(timeshiftlist)>0:
		errorlog.write( event['grid']+','+event['dt'].strftime('%Y-%m-%d,%H%M%S')+"--Not Enough Data\n")
	if len(timeshiftlist)==0:
		errorlog.write( event['grid']+','+event['dt'].strftime('%Y-%m-%d,%H%M%S')+"--No Data\n")
		continue
	medfreqdict=dict()
	for timeshift in timeshiftlist:
		medfreq=round(median(freqdict[str(timeshift)]),4)
		medfreqdict[str(timeshift)]=medfreq
		#print timeshift,medfreq
	eventdt = event['dt'].strftime('_%Y%m%d_%H%M%S')
	#outputpath=event['grid']+'/'
	
	outputfile=event['grid']+eventdt+'.csv'
	#outputfile=eventdt+'.csv'
	with open(outputpath+outputfile,'wb') as outputf:
		csvwriter=csv.writer(outputf)
		csvwriter.writerow(['Time',event['grid']+'_FNET.F'])
		for timeshift in timeshiftlist:
			csvwriter.writerow( [timeshift,medfreqdict[str(timeshift)]])
errorlog.close()			
