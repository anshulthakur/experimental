import numpy as np
import matplotlib.pyplot as plt 
import random

rx_power = random.sample(range(1,30), 500)
scan_freq = range(1,500,1)

plt.plot(scan_freq, rx_power)

plt.show()