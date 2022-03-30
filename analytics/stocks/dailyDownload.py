import os
import sys
import settings
import csv
from datetime import datetime

from stocks.models import Listing, Industry, Stock

from bs4 import BeautifulSoup

import threading
import multiprocessing
import urllib.request, urllib.parse, urllib.error

import time
import brotli
import gzip
from io import BytesIO


error_stocks = []
ERR_FILE = '/tmp/failed_stocks'
#function to parse stock data from URL
def get_data_for_today(obj):
    return get_data_for_date(obj, datetime.today().date())

def get_data_for_date(stock, dateObj):

    def fix_float(value):
        if value.string is not None and len(value.string)>0:
            return float(value.string.replace(',', ''))
        else:
            return 0
    from bs4 import BeautifulSoup
    import urllib.request, urllib.parse, urllib.error
    time.sleep(0.5)
    try:
        listing = Listing.objects.filter(date__contains = dateObj.date(), stock=stock)
        #print(('{da} exists'.format(da = stock.security)))
        if len(listing) == 0:
            #open and read from URL
            try:
                header = {
                            'authority': 'api.bseindia.com',
                            'method': 'GET',
                            'scheme': 'https',
                            'accept': 'application/json, text/plain, */*',
                            'accept-encoding': 'gzip, deflate, br',
                            'accept-language': 'en-IN,en;q=0.9,en-GB;q=0.8,en-US;q=0.7,hi;q=0.6',
                            'dnt': '1',
                            'origin': 'https://www.bseindia.com',
                            'referer': stock.get_quote_url(),
                            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.135 Safari/537.36',
                            }
                req = urllib.request.Request(stock.get_quote_url(), headers=header)
                response = urllib.request.urlopen(req)
                coder = response.headers.get('Content-Encoding', 'utf-8')
                #print('Coder: {}'.format(coder))
                if coder=='br':
                    html_page = brotli.decompress(response.read()).decode('utf-8')
                elif coder=='gzip':
                    buf = BytesIO(response.read())
                    html_page = gzip.GzipFile(fileobj=buf).read().decode('utf-8')
                else:
                    html_page = response.read().decode('utf-8')
                soup = BeautifulSoup(html_page, "html.parser")
                if soup.find('span', id='ContentPlaceHolder1_lblCompanyValue') is None or \
                    soup.find('span', id='ContentPlaceHolder1_lblCompanyValue').find('a') is None:
                    print('Content object not found.')
                    return False
                company_name = soup.find('span', id='ContentPlaceHolder1_lblCompanyValue').find('a').string
                stockdata = soup.find('div', id='ContentPlaceHolder1_divStkData')
                table = stockdata.find('table')
                if table is not None:
                    table_vals = table.findAll('tr',{'class':'TTRow'})
                    if datetime.today().date() >= dateObj.date():
                        for row in table_vals:
                            tds = row('td')
                            if(tds[0].string == dateObj.strftime('%-d/%m/%y')):
                                break
                            else:
                                tds = None
                    else:
                        raise Exception('Cannot fetch data for future')

                    if tds is not None:
                        listing = Listing()
                        listing.stock = stock
                        listing.date = datetime.strptime(tds[0].string, "%d/%m/%y")
                        listing.opening = fix_float(tds[1])
                        listing.high = fix_float(tds[2])
                        listing.low = fix_float(tds[3])
                        listing.closing = fix_float(tds[4])
                        listing.wap = fix_float(tds[5])
                        listing.traded = fix_float(tds[6])
                        listing.trades = fix_float(tds[7])
                        listing.turnover = fix_float(tds[8])
                        listing.deliverable = fix_float(tds[9])
                        #listing.ratio = fix_float(tds[10])
                        #listing.spread_high_low = fix_float(tds[11])
                        #listing.spread_close_open = fix_float(tds[12])
                        listing.save()
                        print(("{code} OK[{d}] [{id}]".format(code=stock.security, d=listing.date, id=listing.id)))
                        return True
                    else:
                        print(("{code} No data available for the day".format(code=stock.security)))
                        return True
            except Exception as e:
                print(("{code} Failed".format(code=stock.security)))
                print(e)
                le = len(sys.exc_info())
                #print(le)
                for i in range(0,le):
                    print(("Unexpected error:", sys.exc_info()[i]))
                return False
        else:
            print('Listing exists for the date')
    except:
        print(("{code} Failed".format(code=stock.security)))
        le = len(sys.exc_info())
        print(le)
        for i in range(0,le):
            print(("Unexpected error:", sys.exc_info()[i]))
        return False

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

def get_next_stock():
    """
    First try to read off IDs from a failure list in case we've run this before
    If file does not exist, then parse entire DB list
    """
    try:
        with open(ERR_FILE, 'r') as fd:
            first = True
            ids = []
            for line in fd:
                if first:
                    if datetime.strptime(line.strip(), "%d/%m/%y").day != datetime.today().day:
                        raise(ReferenceError('Old file'))
                    first = False
                    continue
                ids.append(int(line.strip()))
            if len(ids)==0:
                return
            stocks = Stock.objects.filter(id__in=ids)
            for stock in stocks:
                yield stock
    except FileNotFoundError:
        print('Error file not found')
        stocks = Stock.objects.all()
        for stock in stocks:
            yield stock
    except ReferenceError:
        print('Old file')
        stocks = Stock.objects.all()
        for stock in stocks:
            yield stock
    except Exception as e:
        print(e)
        print ("Error in retreiving stock list and iterating")


def work_loop(thread_name, tid):
    global stock_iter
    for stock in stock_iter:
        #print(day)
        if get_data_for_date(stock, day) is False:
            error_stocks.append(stock)

import os

def read_bhav_file(filename, dateval):
    try:
        with open(filename, 'r') as fd:
            csv_reader = csv.DictReader(fd, delimiter=',')
            i = 0
            for row in csv_reader:
                try:
                    stock = Stock.objects.get(security=row['SC_CODE'])
                    listing = Listing()
                    listing.stock = stock
                    listing.date = dateval
                    listing.opening = fix_float(row['OPEN'])
                    listing.high = fix_float(row['HIGH'])
                    listing.low = fix_float(row['LOW'])
                    listing.closing = fix_float(row['CLOSE'])
                    #listing.wap = fix_float(tds[5])
                    listing.traded = fix_float(row['NO_OF_SHRS'])
                    listing.trades = fix_float(row['NO_TRADES'])
                    listing.turnover = fix_float(row['NET_TURNOV'])
                    #listing.deliverable = fix_float(tds[9])
                    listing.save()
                except Stock.DoesNotExist:
                    print('{} does not exist in DB'.format(row['SC_NAME']))
                    continue
    except IOError as e:
        print(("I/O error({0}): {1}".format(e.errno, e.strerror)))

    pass

def download_bhav_copy(url: str, dest_folder: str):
    from zipfile import ZipFile

    if not os.path.exists(dest_folder):
        os.makedirs(dest_folder)  # create folder if it does not exist

    #filename = url.split('/')[-1].replace(" ", "_")  # be careful with file names
    filename = 'bhav_latest.zip'
    file_path = os.path.join(dest_folder, filename)

    header = {
                            'authority': 'api.bseindia.com',
                            'method': 'GET',
                            'scheme': 'https',
                            'accept': 'application/json, text/plain, */*',
                            'accept-encoding': 'gzip, deflate, br',
                            'accept-language': 'en-IN,en;q=0.9,en-GB;q=0.8,en-US;q=0.7,hi;q=0.6',
                            'dnt': '1',
                            'origin': 'https://www.bseindia.com',
                            'referer': url,
                            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.135 Safari/537.36',
                            }
    req = urllib.request.Request(url, headers=header)
    response = urllib.request.urlopen(req)
    coder = response.headers.get('Content-Encoding', 'utf-8')
    import shutil
    with open(file_path, 'wb') as f:
        shutil.copyfileobj(response, f)
    
    with ZipFile(filename, 'r') as zip:
        # printing all the contents of the zip file
        #zip.printdir()
  
        # extracting all the files
        print('Extracting all the files now...')
        zip.extractall()
        print('Done!')

bhav_url  = 'https://www.bseindia.com/download/BhavCopy/Equity/EQ{datestr}_CSV.ZIP'
dateval = datetime.today() #- timedelta(days = 1)
bhav_file = f"EQ{datetime.today().strftime('%d%m%y')}"
#download_bhav_copy(bhav_url.format(datestr = dateval.strftime("%d%m%y")), dest_folder=".")



day = datetime.today()
import argparse
parser = argparse.ArgumentParser(description='Download stock data for stock/date')
parser.add_argument('-s', '--stock', help="Stock code")
parser.add_argument('-d', '--date', help="Date")
args = parser.parse_args()
stock_code = None

if args.stock is not None and len(args.stock)>0:
    print('Get data for stock {}'.format(args.stock))
    stock_code = int(args.stock)
if args.date is not None and len(args.date)>0:
    print('Get data for date: {}'.format(args.date))
    day = datetime.strptime(args.date, "%d/%m/%y")

if stock_code is None:
    stock_iter = LockedIterator(get_next_stock())

    threads = []
    num_threads = multiprocessing.cpu_count() * 2
    try:
      for thread_id in range(num_threads):
          t = threading.Thread(target=work_loop, args=("Thread ID {id}".format(id=thread_id), thread_id))
          threads.append(t)
          t.start()

      for t in threads:
          t.join()
    except:
      print(("Unable to start thread ({id})".format(id=thread_id)))

    with open(ERR_FILE, 'w') as fd:
        fd.write(datetime.today().strftime("%d/%m/%y"))
        for stock in error_stocks:
            fd.write('\n{}'.format(stock.id))
else:
    try:
        stock = Stock.objects.get(security=stock_code)
        get_data_for_date(stock, day)
    except Stock.DoesNotExist:
        print('Stock does not exist in DB')



