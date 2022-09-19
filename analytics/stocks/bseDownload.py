"""
Parse the CSV listing of all the equity shares of NSE and download their historical data using selenium.

Security Code,Security Id,Security Name,Status,Group,Face Value,ISIN No,Industry,Instrument
"""

import sys
import os
from selenium import webdriver
import csv
import time
from datetime import datetime, timedelta

from selenium import webdriver
from selenium.webdriver.common.by import By

use_chrome = True
if use_chrome:
    from selenium.webdriver.chrome.options import Options
    
    from selenium.webdriver.chrome.service import Service
    from webdriver_manager.chrome import ChromeDriverManager as DriverManager
    from selenium.webdriver import Chrome as Browser
else:
    from selenium.webdriver.edge.service import Service
    from webdriver_manager.microsoft import EdgeChromiumDriverManager  as DriverManager
    from selenium.webdriver import Edge as Browser


import threading
import multiprocessing

num_threads = multiprocessing.cpu_count()
#num_threads = 1
thread_busy = [1 for i in range(0,num_threads)]
stock_codes = None


raw_data_dir = './bseData/'

base_url = "https://www.bseindia.com/markets/equity/EQReports/StockPrcHistori.aspx?expandable=7&scripcode=%s&flag=sp&Submit=G"

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
    def __next__(self):
        self.lock.acquire()
        try:
            return next(self.it)
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
        print(("I/O error({0}): {1}".format(e.errno, e.strerror)))

def error_occured(data):
    if 'internal server error' not in data:
        return False
    else:
        return True

def download_data(driver, url):
    try:
        print(('URL: '+url))
        driver.get(url)
        if not error_occured(driver.find_element(by=By.TAG_NAME, value='body').text):
            to_date_year = driver.find_element(by=By.ID, value='ContentPlaceHolder1_txtToDate').get_attribute('value').split('/')[-1]
            if to_date_year=='':
                return True
            elif int(to_date_year) < 2018:
                #pass because date lies before time of interest
                return True
            from_date = driver.find_element(by=By.ID, value='ContentPlaceHolder1_txtFromDate')
            if from_date is not None and from_date.get_attribute("value") != '':
                from_date.clear()
                #from_date.send_keys('01/01/2007')
                #No longer allows text input from keyboard
                from_date.click()
                month_el = driver.find_element(by=By.CSS_SELECTOR, value='select[class="ui-datepicker-month"]')
                month_el.click()
                month_el.find_element(by=By.CSS_SELECTOR, value='option[value="0"]').click()
                year_el = driver.find_element(by=By.CSS_SELECTOR, value='select[class="ui-datepicker-year"]')
                year_el.click()
                time.sleep(0.1)
                year_el.find_element(by=By.CSS_SELECTOR, value='option[value="2012"]').click()
                year_el = driver.find_element(by=By.CSS_SELECTOR, value='select[class="ui-datepicker-year"]')
                year_el.click()
                time.sleep(0.1)
                year_el.find_element(by=By.CSS_SELECTOR, value='option[value="2007"]').click()
                date_el = driver.find_element(by=By.CSS_SELECTOR, value='table[class="ui-datepicker-calendar"]').find_elements(by=By.TAG_NAME, value='tbody')[0]
                date_elem = date_el.find_element(by=By.XPATH, value=".//*[contains(text(), '1')]")
                date_elem.click()
            else:
                print ('Returned Empty results')
                return True
            #to_date defaults to today. So, we are good!
            try:
                submit_btn = driver.find_element(by=By.ID, value='ContentPlaceHolder1_btnSubmit')
                time.sleep(2.5)
                #print('Click')
                submit_btn.click()
            except:
                submit_btn = driver.find_element(by=By.ID, value='ContentPlaceHolder1_btnSubmit')
                time.sleep(2.5)
                #print('Click')
                submit_btn.click()

            print('Sleep')
            time.sleep(2)
            if not error_occured(driver.find_element(by=By.TAG_NAME, value='body').text):
                download_link = driver.find_element(by=By.ID, value='ContentPlaceHolder1_btnDownload')
                download_link.click()
                return True
            else:
                print ('Error processing')
                return False
        else:
            return False
    except Exception as e:
        print(e)
        print('Exception Geting URL')
        return True

def get_browser_instance():
    options = Options()
    preferences = {"download.default_directory" : os.path.abspath(raw_data_dir),
                   "download.prompt_for_download": False,
                   "download.directory_upgrade": True,
                   "safebrowsing_for_trusted_sources_enabled": False,
                   "safebrowsing.enabled": False
                   }    
    options.add_experimental_option("prefs", preferences)
    #options.add_argument("--headless")
    service = Service(executable_path=DriverManager().install())
    driver = Browser(service=service, options=options)
    driver.implicitly_wait(wait_period)
    return driver

def work_loop(thread_name, tid):
    global thread_busy
    global stock_codes
    
    driver = get_browser_instance()

    print(('{t} Started'.format(t=thread_name)))
    for stock in stock_codes:
        if not os.path.exists(raw_data_dir+"{file_name}".format(file_name=stock.code)+".csv"):
            #time.sleep(1)
            ret_val = download_data(driver, base_url%stock.code)
            while ret_val is not True:
                print(('Retry {name}'.format(name=stock.name)))
                time.sleep(2)
                ret_val = download_data(driver, base_url%stock.code)
        else:
            print(("Skipping %s" %stock.name))
    thread_busy[0] = 0
    print(('{t} done'.format(t=thread_name)))
    driver.close()
    driver.quit()


def download_archive(filename=None, day=None):
    global stock_codes
    try:
        fd = open(filename, 'r')
        stock_codes = LockedIterator(get_next_stock_code(fd))

        threads = []
        try:
            if num_threads >1:
                for thread_id in range(num_threads):
                    t = threading.Thread(target=work_loop, args=("Thread ID {id}".format(id=thread_id), thread_id))
                    threads.append(t)
                    t.start()

                for t in threads:
                    t.join()
            else:
                work_loop("Base", 0)
        except:
            print("Unable to start thread ({id})".format(id=thread_id))

        fd.close()
    except:
        print('Error')

if __name__ == "__main__":
    day = datetime.today()
    wait_period = 5
    import argparse
    parser = argparse.ArgumentParser(description='Download stock data for stock/date')
    parser.add_argument('-d', '--date', help="Date")
    parser.add_argument('-f', '--file', help="List of scrips")
    parser.add_argument('-b', '--bulk', help="Get bulk data for stock(s)", action="store_true", default=False)
    args = parser.parse_args()
    stock_code = None
    
    if args.date is not None and len(args.date)>0:
        print('Get data for date: {}'.format(args.date))
        day = datetime.strptime(args.date, "%d/%m/%y")
    if args.bulk:
        bulk = True

    if args.file is None or len(args.file)==0:
        print("Provide a scrip file name")
        exit()
    elif os.path.isfile(args.file) is False:
        #Skip file download
        print(f"File {args.file} does not exist")
        exit()

    try:
        os.mkdir(raw_data_dir)
    except FileExistsError:
        pass
    except:
        print('Error creating raw data folder')
        exit(0)

    if len(sys.argv) < 2:
        print ("Insufficient Parameters")
        print(("Usage: %s <csv file of equity list>" %sys.argv[0]))
        exit()

    if args.bulk:    
        download_archive(filename=args.file)
    else:
        download_archive(filename=args.file, date=day.date())
