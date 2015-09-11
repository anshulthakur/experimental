import os
import sys
import re
import numpy as np
import matplotlib.pyplot as plt 

if len(sys.argv) < 0:
	print "No file supplied"
	exit()

filename = sys.argv[1]
fd = open(filename, 'r')
#2015-09-03 08:27:33.588183 center_freq 460900000.0 freq 460000000.0 power_db 5.59471050376 noise_floor_db -78.5314550789
line_regex = re.compile(r'.*center_freq\s+(?P<scan_freq>\d+\.\d+)\s+.*power_db\s+(?P<rx_power>\d+\.\d+)\s.*$')
rx_power = []
scan_freq = []
freq = 0
i = 0
while 1:
	line = fd.readline()
	if not line:
		break
	line_match = line_regex.match(line)
	if line_match is not None:
		#Extract the patterns of Received Power and Frequency
		rx_power.append(float(line_match.group('rx_power')))
		scan_freq.append(float(line_match.group('scan_freq')))
		if freq > scan_freq[-1]:
			scan_freq.pop()
			rx_power.pop()
			break
		freq = scan_freq[-1]
		print('{freq}\t{power}\n'.format(power=rx_power[-1], freq=scan_freq[-1]))
		i+=1
plt.plot(scan_freq, rx_power)

plt.show()
