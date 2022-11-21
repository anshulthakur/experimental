import os
import sys
import settings
import csv
from datetime import datetime, timedelta

from stocks.models import Listing, Industry, Stock

import requests

import time
import brotli
import gzip
from io import BytesIO
from zipfile import ZipFile


#from selenium import webdriver
#from selenium.webdriver.common.by import By

#use_chrome = True
#if use_chrome:
#    from selenium.webdriver.chrome.options import Options
#    
#    from selenium.webdriver.chrome.service import Service
#    from webdriver_manager.chrome import ChromeDriverManager as DriverManager
#    from selenium.webdriver import Chrome as Browser
#else:
#    from selenium.webdriver.edge.service import Service
#    from webdriver_manager.microsoft import EdgeChromiumDriverManager  as DriverManager
#    from selenium.webdriver import Edge as Browser


raw_data_dir = './nseData/'
delivery_data_dir = raw_data_dir+'delivery/'

archive_url = 'https://www.nseindia.com/all-reports'

months = ['', 'JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
def handle_download(session, url, filename, path=raw_data_dir):
    print(url)
    if os.path.isfile(path+filename):
        #Skip file download
        return
    time.sleep(1)
    try:
        response = session.get(url, timeout=10)
    except requests.exceptions.TooManyRedirects:
        print('Data may not be available')
        return
    except requests.exceptions.Timeout:
        #Skip file download
        print('Timeout')
        return
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
            with open(path+filename, 'w') as fd:
                fd.write(result)
        else:
            with open(path+filename, 'wb') as fd:
                fd.write(result)
            
    else:
        #print(response.content.decode('utf-8'))
        print('Received textual data')
        pass
        

def clean_delivery_data(filename):
    skip = 4
    newfile = filename.replace('DAT', 'csv')
    with open(newfile, 'w') as d_fd:
        d_fd.write("Record Type,Sr No,Name of Security,Type,Quantity Traded,Deliverable Quantity,Percentage\n")
        with open(filename, 'r') as fd:
            for row in fd:
                if skip >0:
                    skip -= 1
                    continue
                d_fd.write(row)
    os.remove(filename)

def download_archive(date = datetime.strptime('20-04-2012', "%d-%m-%Y").date()):
    #driver.get(archive_url)

    session = requests.Session()
    # Set correct user agent
    #selenium_user_agent = driver.execute_script("return navigator.userAgent;")
    selenium_user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36 Edg/107.0.1418.35'
    #print(selenium_user_agent)
    session.headers.update({"user-agent": selenium_user_agent})
    session.headers.update({"accept-encoding": "gzip, deflate, br",
            "accept":
    """text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9""",
            "accept-language": "en-US,en;q=0.9",
            "host": "www.nseindia.com",
            "referer": "https://www.nseindia.com"})

    base_bhav_file_csv = 'cm{day:02}{month}{year:04}bhav.csv'
    base_bhav_file = base_bhav_file_csv+'.zip'
    #base_url_bhav = 'https://www1.nseindia.com/content/historical/EQUITIES/{year:04}/{month}/'+base_bhav_file
    base_url_bhav = 'https://archives.nseindia.com/content/historical/EQUITIES/{year:04}/{month}/'+base_bhav_file

    base_delivery_file = 'MTO_{day:02}{month:02}{year:04}.DAT'
    #base_delivery_url = 'https://www1.nseindia.com/archives/equities/mto/'+base_delivery_file
    base_delivery_url = 'https://archives.nseindia.com/archives/equities/mto/'+base_delivery_file
    

    #el = driver.find_element(by = By.XPATH, value='//*[@id="spanDisplayBox"]/table/tbody/tr/td/a')
    #if el is not None:
    #   url = el.get_attribute('href')
    while date <= datetime.today().date():
        if date.weekday()<=4:
            print(f'Downloading for {date}')
            #Download bhavcopy
            if os.path.exists(raw_data_dir+base_bhav_file_csv.format(day=date.day, month=months[date.month], year=date.year)) and \
                os.path.exists(delivery_data_dir+base_delivery_file.replace('DAT', 'csv').format(day=date.day, month=date.month, year=date.year)):
                pass
            else:
                session.headers.update({"host": "www.nseindia.com"})
                session.get(archive_url)
                session.headers.update({"host": "archives.nseindia.com"})
            if os.path.exists(raw_data_dir+base_bhav_file_csv.format(day=date.day, month=months[date.month], year=date.year)):
                pass
            else:
                handle_download(session, url = base_url_bhav.format(day=date.day, month=months[date.month], year=date.year), 
                                    filename = base_bhav_file.format(day=date.day, month=months[date.month], year=date.year))
                #Bhavcopy is zip file, so handle that
                if os.path.isfile(raw_data_dir+base_bhav_file.format(day=date.day, month=months[date.month], year=date.year)):
                    with ZipFile(raw_data_dir+base_bhav_file.format(day=date.day, month=months[date.month], year=date.year), 'r') as zipf:
                        # printing all the contents of the zip file
                        #zipf.printdir()
                        # extracting all the files
                        #print('Extracting all the files now...')
                        zipf.extractall(raw_data_dir)
                        #print('Done!')
                    os.remove(raw_data_dir+base_bhav_file.format(day=date.day, month=months[date.month], year=date.year))
                    
            #Download delivery data
            if os.path.exists(delivery_data_dir+base_delivery_file.format(day=date.day, month=date.month, year=date.year)):
                clean_delivery_data(delivery_data_dir+base_delivery_file.format(day=date.day, month=date.month, year=date.year))
            elif os.path.exists(delivery_data_dir+base_delivery_file.replace('DAT', 'csv').format(day=date.day, month=date.month, year=date.year)):
                pass
            else:
                handle_download(session, url = base_delivery_url.format(day=date.day, month=date.month, year=date.year), 
                                    filename = base_delivery_file.format(day=date.day, month=date.month, year=date.year),
                                    path=delivery_data_dir)
                if os.path.exists(delivery_data_dir+base_delivery_file.format(day=date.day, month=date.month, year=date.year)):
                    clean_delivery_data(delivery_data_dir+base_delivery_file.format(day=date.day, month=date.month, year=date.year))

        date = date + timedelta(days=1)

def get_scrip_list(offline=False):
    url = 'https://archives.nseindia.com/content/equities/EQUITY_L.csv'
    filename = 'NSE_list.csv'
    session = requests.Session()
    user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36'
    session.headers.update({"user-agent": user_agent})
    session.headers.update({"accept-encoding": "gzip, deflate, br",
            "accept":
    """text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9""",
            "accept-language": "en-GB,en;q=0.9,en-US;q=0.8",
            "authority": "www.nseindia.com",
            "referrer": "www.nseindia.com"})
    if not offline:
        try:
            session.get('https://www.nseindia.com/market-data/securities-available-for-trading')
            handle_download(session, 
                            url = url, 
                            filename = filename,
                            path='./')
        except:
            print("Could not download latest ")
            pass
    
    if os.path.exists('./'+filename):
        print(f'File may be outdated. Download latest copy from: {url}')
    else:
        print(f'NSE list of scrips does not exist in location. Download from: {url}')

    members = []
    if os.path.exists('./'+filename):
        with open(filename,'r') as fd:
            reader = csv.DictReader(fd)
            for row in reader:
                members.append({'symbol': row['SYMBOL'].upper().strip(),
                                'name':row['NAME OF COMPANY'].upper().strip(),
                                'isin': row[' ISIN NUMBER'].upper().strip(),
                                'facevalue': row[' FACE VALUE'].upper().strip()
                                })
    return members

def populate_scrips():
    scrip_list = 'NSE_list.csv'
    with open(scrip_list, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            try:
                market = Market.objects.get(name = 'NSE')
                industry = None
                try:
                    stock = Stock.objects.get(isin=row.get('ISIN NUMBER'))
                except Stock.DoesNotExist:
                    stock = Stock(sid = row.get('SYMBOL'),
                                  name = row.get('NAME OF COMPANY').strip(),
                                  group = row.get('SERIES').strip(),
                                  face_value = row.get('FACE VALUE').strip(),
                                  isin = row.get('ISIN NUMBER').strip(),
                                  industry = industry,
                                  market = market)
                    stock.save()
                    print('Created {}'.format(stock.sid))
            except:
                pass
                    
def add_data_to_db(date = datetime.strptime('01-01-2005', "%d-%m-%Y").date()):
    base_bhav_file = 'cm{day:02}{month}{year:04}bhav.csv'
    base_delivery_file = 'MTO_{day:02}{month:02}{year:04}.DAT'

    while date <= datetime.today().date():
        stocks = []
        listings = []
        bhav_name = raw_data_dir+base_bhav_file.format(day=date.day, month=months[date.month], year=date.year)
        if os.path.isfile(bhav_name):
            with open(bhav_name, 'r') as bhavfile:
                reader = csv.DictReader(bhavfile)
                for row in reader:
                    try:
                        stock = Stock.objects.get(isin=row.get('ISIN'))
                    except Stock.DoesNotExist:
                        stock = Stock(isin = row.get('ISIN'),
                                      sid = row.get('SYMBOL'))
                    listing = Listing(stock=stock,
                                          date=datetime.strptime(row.get('Date').strip(), '%d-%B-%Y'),
                                          open=float(row.get('Open Price').strip()),
                                          high = float(row.get('High Price').strip()),
                                          low = float(row.get('Low Price').strip()),
                                          close = float(row.get('Close Price').strip()),
                                          wap = float(row.get('WAP').strip()),
                                          traded = int(row.get('No.of Shares').strip()),
                                          trades = int(row.get('No. of Trades').strip()),
                                          turnover = float(row.get('Total Turnover (Rs.)').strip()),
                                          deliverable = float(row.get('Deliverable Quantity').strip()) if len(row.get('Deliverable Quantity').strip()) > 0 else 0,
                                          )
                    listings += [listing]
    pass

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

    try:
        os.mkdir(raw_data_dir)
        os.mkdir(delivery_data_dir)
    except FileExistsError:
        pass
    except:
        print('Error creating raw data folder')
        exit(0)
    #options = Options()
    #preferences = {"download.default_directory" : raw_data_dir}
    #options.add_experimental_option("prefs", preferences)
    #options.add_argument("--headless")

    #service = Service(executable_path=DriverManager().install())
    #driver = Browser(service=service, options=options)
    #driver.implicitly_wait(wait_period)
    #driver = None
    if args.bulk:    
        download_archive()
    else:
        download_archive(date=day.date())
