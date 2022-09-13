import os
import sys
import settings
import csv
from datetime import datetime

from stocks.models import Listing, Stock

from bs4 import BeautifulSoup

import threading
import multiprocessing
import urllib.request

import time
import brotli
import gzip
from io import BytesIO

import traceback

error_stocks = []
ERR_FILE = '/tmp/failed_stocks'

def fix_float(value):
    if value.string is not None and len(value.string)>0 and value.string.strip()!='-':
        return float(value.string.replace(',', ''))
    else:
        return 0
    
#function to parse stock data from URL
def get_data_for_today(obj):
    return get_data_for_date(obj, datetime.today().date())

def get_data_for_date(stock, dateObj):

    
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

def get_bulk(stock):
    time.sleep(0.5)
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
        stockdata = soup.find('div', id='ContentPlaceHolder1_divStkData')
        table = stockdata.find('table')
        if table is not None:
            table_vals = table.findAll('tr',{'class':'TTRow'})
            for row in table_vals:
                tds = row('td')
                if tds is not None:
                    listing = Listing.objects.filter(date__contains = datetime.strptime(tds[0].string, "%d/%m/%y").date(), stock=stock)
                    #print(('{da} exists'.format(da = stock.security)))
                    if len(listing) == 0:
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
                    else:
                        if fix_float(tds[9])>0 and listing[0].deliverable==0:
                            #Update the delivery data
                            print('Update delivery data')
                            listing[0].deliverable = fix_float(tds[9])
                            listing[0].save()
                        else:
                            print('Listing exists for the date')
                else:
                    print(("{code} No data available for the days".format(code=stock.security)))
                    return True
    except Exception as e:
        print(("{code} Failed".format(code=stock.security)))
        print(e)
        le = len(sys.exc_info())
        #print(le)
        for i in range(0,le):
            print(("Unexpected error:", sys.exc_info()[i]))
        return False


#For thread safe operation using locks
class LockedIterator(object):
    def __init__(self, it):
        self.lock = threading.Lock()
        self.it = it.__iter__()
    def __iter__(self): return self
    #Python3
    def __next__(self):
        self.lock.acquire()
        try:
            return self.it.__next__()
        finally:
            self.lock.release()

def get_next_stock(override=False):
    """
    First try to read off IDs from a failure list in case we've run this before
    If file does not exist, then parse entire DB list
    """
    try:
        if not override:
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
        else:
            raise(ReferenceError('Override set'))
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
        if bulk:
            get_bulk(stock)
        else:
            if get_data_for_date(stock, day) is False:
                error_stocks.append(stock)

def read_bhav_file(filename, dateval):
    try:
        with open(filename, 'r') as fd:
            csv_reader = csv.DictReader(fd, delimiter=',')
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
    import shutil
    with open(file_path, 'wb') as f:
        shutil.copyfileobj(response, f)
    
    with ZipFile(filename, 'r') as zipf:
        # printing all the contents of the zip file
        #zipf.printdir()
        # extracting all the files
        print('Extracting all the files now...')
        zipf.extractall()
        print('Done!')

bhav_url  = 'https://www.bseindia.com/download/BhavCopy/Equity/EQ{datestr}_CSV.ZIP'
dateval = datetime.today() #- timedelta(days = 1)
bhav_file = f"EQ{datetime.today().strftime('%d%m%y')}"
#download_bhav_copy(bhav_url.format(datestr = dateval.strftime("%d%m%y")), dest_folder=".")

def download_index_report(dest_folder: str = './indices/reports/', date=datetime.today()):
    if not os.path.exists(dest_folder):
        os.makedirs(dest_folder)  # create folder if it does not exist
        
    #url = 'https://www1.nseindia.com/content/indices/ind_close_all_{}.csv'.format(date.strftime("%d%m%Y"))
    url = 'https://www1.nseindia.com/homepage/Indices1.json'
    filename = 'ind_close_all_{}.csv'.format(date.strftime("%d%m%Y"))
    file_path = os.path.join(dest_folder, filename)
    
    print('URL: '+url)

    header = {
                'method': 'GET',
                'scheme': 'https',
                #'authority': 'nseindia.com',
                'accept': 'application/json, text/plain, */*',
                'accept-encoding': 'gzip, deflate, br',
                'accept-language': 'en-US,en;q=0.9',
                'dnt': '1',
                'host': 'www1.nseindia.com',
                'referer': 'https://www1.nseindia.com/products/content/equities/indices/homepage_indices.htm',
                'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.135 Safari/537.36',
                }
    try:
        #print(header)
        req = urllib.request.Request(url, headers=header)
        response = urllib.request.urlopen(req)
        coder = response.headers.get('Content-Encoding', 'utf-8')
        #print('Coder: {}'.format(coder))
        if coder=='br':
            html_page = brotli.decompress(response.read()).decode('utf-8')
        elif coder=='gzip':
            buf = BytesIO(response.read())
            html_page = gzip.GzipFile(fileobj=buf).read().decode('utf-8')
            print('Got gzip response')
        else:
            html_page = response.read().decode('utf-8')
        import shutil
        with open(file_path, 'wb') as f:
            shutil.copyfileobj(html_page, f)
            print('Downloaded')
    except:
        traceback.print_exc()
        pass

INDEX_MAP = {"Nifty 50":"nifty50.csv",
            "Nifty Auto":"auto.csv",
            "Nifty Bank":"banknifty.csv",
            "Nifty Energy":"energy.csv",
            "Nifty Financial Services":"finance.csv",
            "Nifty FMCG":"fmcg.csv",
            "Nifty IT":"niftyit.csv",
            "Nifty Media":"niftymedia.csv",
            "Nifty Metal":"metal.csv",
            "Nifty Pharma":"pharma.csv",
            "Nifty PSU Bank":"psubank.csv",
            "Nifty Realty":"realty.csv",
            "Nifty India Consumption":"consumption.csv",
            "Nifty Commodities":"commodities.csv",
            "Nifty Infrastructure":"infra.csv",
            "Nifty Services Sector":"services.csv",
            "Nifty Non-Cyclical Consumer":"noncyclical.csv",
            "Nifty India Manufacturing":"manufacturing.csv",
            "Nifty Oil & Gas":"oilgas.csv",
            "Nifty Healthcare Index":"healthcare.csv",
            "Nifty India Digital":"digital.csv",
            "Nifty India Defence":"defense.csv",
            "Nifty Financial Services Ex-Bank":"finexbank.csv",
            "Nifty Housing":"housing.csv",
            "Nifty Transportation & Logistics":"logistics.csv",
            "Nifty Consumer Durables":"consumer.csv",
            "Nifty Private Bank":"pvtbank.csv",}

def update_indices(date=datetime.today()):
    fname = './indices/reports/ind_close_all_{}.csv'.format(date.strftime("%d%m%Y"))
    from csv import writer
    with open(fname, 'r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            dest_idx = row['Index Name']
            if dest_idx in INDEX_MAP:
                print('Update {}'.format(dest_idx.strip()))
                with open('./indices/'+INDEX_MAP[dest_idx.strip()], 'a', newline='') as f_object:
                    # Pass the CSV  file object to the writer() function
                    writer_object = writer(f_object)
                    # Result - a writer object
                    # Pass the data in the list as an argument into the writerow() function
                    writer_object.writerow([date.strftime('%d-%b-%y'), 
                                            row['Open Index Value'],
                                            row['High Index Value'],
                                            row['Low Index Value'],
                                            row['Closing Index Value'],
                                            row['Volume'],
                                            row['Turnover (Rs. Cr.)'],])
            else:
                print(f'{dest_idx} is not mapped')
            

if __name__ == "__main__":
    day = datetime.today()
    bulk  = False
    import argparse
    parser = argparse.ArgumentParser(description='Download stock data for stock/date')
    parser.add_argument('-s', '--stock', help="Stock code")
    parser.add_argument('-r', '--report', help="Index report", action='store_true', default=False)
    parser.add_argument('-d', '--date', help="Date")
    parser.add_argument('-b', '--bulk', help="Get bulk data for stock(s)", action="store_true", default=False)
    args = parser.parse_args()
    stock_code = None
    
    if args.stock is not None and len(args.stock)>0:
        print('Get data for stock {}'.format(args.stock))
        stock_code = int(args.stock)
    if args.date is not None and len(args.date)>0:
        print('Get data for date: {}'.format(args.date))
        day = datetime.strptime(args.date, "%d/%m/%y")
    if args.report is True:
        print('Download index report')
        update_indices(date = day)
        exit()
    if args.bulk:
        bulk = True
        
    if stock_code is None:
        stock_iter = LockedIterator(get_next_stock(args.bulk))
    
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
            if args.bulk:
                get_bulk(stock)
            else:
                get_data_for_date(stock, day)
        except Stock.DoesNotExist:
            print('Stock does not exist in DB')
    




