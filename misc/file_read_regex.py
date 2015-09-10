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

line_regex = re.compile(r'\w\s+\w\s(?P<scan_freq>\d+)\s+(?P<rx_power>\d+)\s.*$')
rx_power = []
scan_freq = []
i = 0
while 1:
	line = fd.readline()
	if not line:
		break
	line_match = line_regex.match(line)
	if line_match is not None:
		#Extract the patterns of Received Power and Frequency
		rx_power[i] = line_match.group('rx_power')
		scan_freq[i] = line_match.group('scan_freq')
		i++
plt.plot(scan_freq, rx_power)

plt.show()