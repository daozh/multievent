import matplotlib.pyplot as plt
import sys
if len(sys.argv)>1:
	filename=str(sys.argv[1])
#filename ='EI_20160301_175555.csv'
print filename
freq=[]
tstamp=[]
with open(filename, 'rb') as datafile:
	for line in datafile:
		data=line.split(",")
		if data[0] <> 'Time':
			freq.append(data[1])
			tstamp.append(data[0])

plt.plot(tstamp,freq)
plt.ticklabel_format(style='plain', axis='y',scilimits=(0,0))
plt.ylabel('Frequency (Hz)')
plt.xlabel('Time (second)')
plt.grid('on')
plt.savefig(filename[:-3])
plt.show()

