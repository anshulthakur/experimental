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


def get_next_stock_code(filename=None):
	try:
		fd = open(filename, 'rb')
		print 'File opened'
		csv_reader = csv.DictReader(fd, delimiter=',')
		i = 0
		for row in csv_reader:
			yield Stock(row)
		fd.close()
	except IOError as e:
		print "I/O error({0}): {1}".format(e.errno, e.strerror)

def error_occured(data):
	if 'internal server error' not in data:
		return False
	else:
		return True

def download_data(driver, url):
	driver.get(url)
	if not error_occured(driver.find_element_by_tag_name('body').text):
		from_date = driver.find_element_by_id('ctl00_ContentPlaceHolder1_txtFromDate')
		if from_date is not None:
			from_date.clear()
			from_date.send_keys('01/01/2007')
		#to_date defaults to today. So, we are good!
		submit_btn = driver.find_element_by_id('ctl00_ContentPlaceHolder1_btnSubmit')
		time.sleep(1)
		submit_btn.click()

		if not error_occured(driver.find_element_by_tag_name('body').text):
			download_link = driver.find_element_by_id('ctl00_ContentPlaceHolder1_btnDownload')
			download_link.click()
			return True
		else:
			return False
	else:
		return False

if len(sys.argv) < 2:
	print "Insufficient Parameters"
	print "Usage: %s <csv file of equity list>" %sys.argv[0]
	exit()

stock_codes = get_next_stock_code(sys.argv[1])

options = webdriver.ChromeOptions() 
download_dir = "/home/anshul/web/analytics/analytics/stocks/bsedata/"
preferences = {"download.default_directory" : download_dir}
#options.add_argument("")
options.add_experimental_option("prefs", preferences)
driver = webdriver.Chrome(chrome_options=options)
driver.implicitly_wait(10) #seconds: After page load, some classes may load up by JS. So, wait if not available



base_url = "http://www.bseindia.com/markets/equity/EQReports/StockPrcHistori.aspx?expandable=7&scripcode=%s&flag=sp&Submit=G"
for stock in stock_codes:
	if not os.path.exists(download_dir+"{file_name}".format(file_name=stock.code)+".csv"):
		time.sleep(1)
		ret_val = download_data(driver, base_url%stock.code)
		while ret_val is not True:
			print 'Retry {name}'.format(name=stock.name)
			time.sleep(2)
			ret_val = download_data(driver, base_url%stock.code)
	else:
		print "Skipping %s" %stock.name
