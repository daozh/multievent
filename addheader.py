#Add headers to the csvfiles
import glob
import re

grid='QI'
datafiles=glob.glob(grid+'/*.csv')
for filename in datafiles:	
	with open(filename, 'rb') as oldfile:
		with open('datafiles/'+filename,'wb') as newfile:
			newfile.write('Time,'+grid+'_FNET.F\r\n')
			for line in oldfile:
				newfile.write(line)