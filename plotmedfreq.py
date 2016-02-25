import matplotlib.pyplot as plt
filename ='EI_20070804_214332.csv'
freq=[]
tstamp=[]
with open(filename, 'rb') as datafile:
	for line in datafile:
		data=line.split(",")
		freq.append(data[1])
		tstamp.append(data[0])
		#freq=freq.append(data[1])
plt.plot(tstamp,freq)
plt.ylabel('Frequency (Hz)')
plt.xlabel('Time (second)')
plt.grid('on')
plt.savefig(filename[:-3])
plt.show()

