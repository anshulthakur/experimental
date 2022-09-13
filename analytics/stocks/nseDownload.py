import os
import sys
import settings
import csv
from datetime import datetime, timedelta

from stocks.models import Listing, Industry, Stock

from bs4 import BeautifulSoup

import threading
import multiprocessing
import requests

import time
import brotli
import gzip
from io import BytesIO

import traceback

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


raw_data_dir = './nseData/'

archive_url = 'https://www1.nseindia.com/products/content/equities/equities/archieve_eq.htm'

months = ['', 'JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
def handle_download(session, url, filename):
    print(url)
    
    response = session.get(url)
    #print(response.headers)
    text = False
    if response.status_code==200:
        coder = response.encoding
        try:
            #if coder=='br':
            result = brotli.decompress(response.content).decode('utf-8')
        except:
            #elif coder=='gzip':
            try:
                buf = BytesIO(response.content)
                result = gzip.GzipFile(fileobj=buf).read().decode('utf-8')
            except:
                #else:
                try:
                    result = response.content.decode('utf-8')
                    text=True
                except:
                    result = response.content
        if text:
            with open(raw_data_dir+filename, 'w') as fd:
                fd.write(result)
        else:
            with open(raw_data_dir+filename, 'wb') as fd:
                fd.write(result)
    else:
        print(response.content.decode('utf-8'))

def download_archive(driver):
    driver.get(archive_url)

    session = requests.Session()
    # Set correct user agent
    selenium_user_agent = driver.execute_script("return navigator.userAgent;")
    #print(selenium_user_agent)
    session.headers.update({"user-agent": selenium_user_agent})
    session.headers.update({"accept-encoding": "gzip, deflate, br",
            "accept":
    """text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9""",
            "accept-language": "en-GB,en;q=0.9,en-US;q=0.8",
            "host": "www1.nseindia.com",
            "referer": "https://www1.nseindia.com/products/content/equities/equities/archieve_eq.htm"})

    for cookie in driver.get_cookies():
        #print(cookie)
        session.cookies.set(cookie['name'], cookie['value'], domain=cookie['domain'])
    
    #time.sleep(2)
    #search = driver.find_element(by=By.CLASS_NAME, value='archive_search')
    #Set date
    #time.sleep(1)
    #search.find_element(by =By.ID, value="date").send_keys('04-01-2022')
    #time.sleep(1)
    #Select bhavcopy
    #search.find_element(by=By.ID, value="h_filetype").click()
    #time.sleep(1)
    #search.find_element(by=By.CSS_SELECTOR, value='option[value="eqbhav"]').click()
    #time.sleep(1)
    #click 
    #print(search.find_element(by=By.CLASS_NAME, value="getdata-button"))
    #search.find_element(by=By.CLASS_NAME, value="getdata-button").click()
    time.sleep(2)
    base_bhav_file = 'cm{day:02}{month}{year:04}bhav.csv.zip'
    base_url_bhav = 'https://www1.nseindia.com/content/historical/EQUITIES/{year:04}/{month}/'+base_bhav_file

    base_delivery_file = 'MTO_{day:02}{month:02}{year:04}.DAT'
    base_delivery_url = 'https://www1.nseindia.com/archives/equities/mto/'+base_delivery_file
    date = datetime.strptime('01-01-2005', "%d-%m-%Y").date()

    #el = driver.find_element(by = By.XPATH, value='//*[@id="spanDisplayBox"]/table/tbody/tr/td/a')
    #if el is not None:
    #   url = el.get_attribute('href')
    while date <= datetime.today().date():
        if date.weekday()<=4:
            print(f'Downloading for {date}')
            #Download bhavcopy
            handle_download(session, url = base_url_bhav.format(day=date.day, month=months[date.month], year=date.year), 
                                    filename = base_bhav_file.format(day=date.day, month=months[date.month], year=date.year))
            #Download delivery data
            #handle_download(session, url = base_delivery_url.format(day=date.day, month=date.month, year=date.year), 
            #                        filename = base_delivery_file.format(day=date.day, month=date.month, year=date.year))
        date = date + timedelta(days=1)
    


if __name__ == "__main__":
    day = datetime.today()
    wait_period = 5
    import argparse
    parser = argparse.ArgumentParser(description='Download stock data for stock/date')
    parser.add_argument('-d', '--date', help="Date")
    parser.add_argument('-b', '--bulk', help="Get bulk data for stock(s)", action="store_true", default=False)
    args = parser.parse_args()
    stock_code = None
    
    if args.date is not None and len(args.date)>0:
        print('Get data for date: {}'.format(args.date))
        day = datetime.strptime(args.date, "%d/%m/%y")
    if args.bulk:
        bulk = True

    options = Options()
    preferences = {"download.default_directory" : raw_data_dir}
    options.add_experimental_option("prefs", preferences)

    service = Service(executable_path=DriverManager().install())
    driver = Browser(service=service, options=options)
    driver.implicitly_wait(wait_period)
    download_archive(driver)
    
    driver.quit()