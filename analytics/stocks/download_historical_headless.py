"""
Parse the CSV listing of all the equity shares of NSE and download their historical data using selenium.

Security Code,Security Id,Security Name,Status,Group,Face Value,ISIN No,Industry,Instrument
"""

import sys
import os
from selenium import webdriver
import csv
import time
from selenium.webdriver.chrome.options import Options

import threading
import multiprocessing

num_threads = multiprocessing.cpu_count()
thread_busy = [1 for i in range(0,num_threads)]

download_dir = "/home/craft/web/analytics/analytics/stocks/bsedata/"
preferences = {"download.default_directory" : download_dir}

base_url = "http://www.bseindia.com/markets/equity/EQReports/StockPrcHistori.aspx?expandable=7&scripcode=%s&flag=sp&Submit=G"

class Stock(object):
	def __init__(self, params=None):
		if params is not None:
			self.code = int(params['Security Code'])
			self.id = params['Security Id']
			self.name = params['Security Name']
			self.status = params['Status']
			self.group = params['Group']
			self.face_value=float(params['Face Value'])
			self.isin = params['ISIN No']
			self.industry = params['Industry']
			self.instrument = params['Instrument']

#For thread safe operation using locks			
class LockedIterator(object):
    def __init__(self, it):
        self.lock = threading.Lock()
        self.it = it.__iter__()

    def __iter__(self): return self

    #Python2
    def next(self):
        self.lock.acquire()
        try:
            return self.it.next()
        finally:
            self.lock.release()
    #Python3
    def __next__(self):
        self.lock.acquire()
        try:
            return self.it.__next__()
        finally:
            self.lock.release()

def get_next_stock_code(fd=None):
	try:
		csv_reader = csv.DictReader(fd, delimiter=',')
		i = 0
		for row in csv_reader:
			yield Stock(row)
	except IOError as e:
		print ("I/O error({0}): {1}".format(e.errno, e.strerror))

def error_occured(data):
	if 'internal server error' not in data:
		return False
	else:
		return True

def download_data(driver, url):
	try:
		print('URL: '+url)
		driver.get(url)
		if not error_occured(driver.find_element_by_tag_name('body').text):
			from_date = driver.find_element_by_id('ctl00_ContentPlaceHolder1_txtFromDate')
			if from_date is not None and from_date.get_attribute("value") is not u'':
				from_date.clear()
				from_date.send_keys('01/01/2007')
			else:
				print ('Returned Empty results')
				return True
			#to_date defaults to today. So, we are good!
			submit_btn = driver.find_element_by_id('ctl00_ContentPlaceHolder1_btnSubmit')
			time.sleep(1)
			submit_btn.click()

			if not error_occured(driver.find_element_by_tag_name('body').text):
				download_link =  driver.find_element_by_id('ctl00_ContentPlaceHolder1_btnDownload')
				download_link.click()
				return True
			else:
				print ('Error processing')
				return False
		else:
			return False
	except:
		print ('Exception Geting URL')
		return False

def work_loop(thread_name, tid):
	global thread_busy
	global stock_codes

	print ('{t} Started'.format(t=thread_name))
	options = webdriver.ChromeOptions() 
	
	#options.add_argument("")
	options.add_experimental_option("prefs", preferences)
	#options.add_argument('headless')
	#options.add_argument('window-size=0x0')
	driver = webdriver.Chrome('/home/craft/Downloads/chromedriver_linux64/chromedriver',chrome_options=options)
	driver.implicitly_wait(10) #seconds: After page load, some classes may load up by JS. So, wait if not available

	for stock in stock_codes:
		if not os.path.exists(download_dir+"{file_name}".format(file_name=stock.code)+".csv"):
			#time.sleep(1)
			ret_val = download_data(driver, base_url%stock.code)
			while ret_val is not True:
				print ('Retry {name}'.format(name=stock.name))
				time.sleep(2)
				ret_val = download_data(driver, base_url%stock.code)
		else:
			print ("Skipping %s" %stock.name)
	thread_busy[0] = 0
	print ('{t} done'.format(t=thread_name))
	driver.close()
	driver.quit()



if len(sys.argv) < 2:
	print ("Insufficient Parameters")
	print ("Usage: %s <csv file of equity list>" %sys.argv[0])
	exit()

try:
	fd = open(sys.argv[1], 'r')
	print ('File opened')
	stock_codes = LockedIterator(get_next_stock_code(fd))

	# try:
	# 	for thread_id in range(1,num_threads):
	# 		thread.start_new_thread(work_loop, ("Thread ID {id}".format(id=thread_id), thread_id))
	# except:
	# 	print "Unable to start thread ({id})".format(id=thread_id)

	threads = []
	try:
		for thread_id in range(num_threads):
		    t = threading.Thread(target=work_loop, args=("Thread ID {id}".format(id=thread_id), thread_id))
		    threads.append(t)
		    t.start()

		for t in threads:
		    t.join()
	except:
		print ("Unable to start thread ({id})".format(id=thread_id)	    )

	fd.close()
except:
	print('Error')
#work_loop("Thread ID 0", 0)

#while 1 in thread_busy:
#	pass #Loop until all threads are done
