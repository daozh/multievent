#remove empty lines in data files
import glob
import re

datafiles=glob.glob('TI/*.csv')
for filename in datafiles:	
	with open(filename, 'rb') as oldfile:
		with open('datafiles/'+filename,'wb') as newfile:
			for line in oldfile:
				newline=re.sub('[\n\r\t]','',line)
				newfile.write(newline+'\r\n')