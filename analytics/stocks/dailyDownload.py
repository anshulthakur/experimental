import os
import sys
import settings
import csv
from datetime import datetime

from stocks.models import Listing, Industry, Stock

from bs4 import BeautifulSoup

import threading
import multiprocessing
import urllib

#function to parse stock data from URL
def get_data_for_today(obj):
    return get_data_for_date(obj, datetime.today().date())
    
def get_data_for_date(stock, dateObj):
    from bs4 import BeautifulSoup
    import urllib
    try:
        listing = Listing.objects.filter(date__contains = dateObj.date(), stock=stock)
        print('{da} exists'.format(da = stock.security))
        if len(listing) == 0:
            #open and read from URL
            try:
                response = urllib.request.urlopen(stock.get_quote_url())
                html_page = response.read()
                soup = BeautifulSoup(html_page, "html.parser")
                company_name = soup.find('span', id='ctl00_ContentPlaceHolder1_lblCompanyValue').find('a').string
                stockdata = soup.find('span', id='ctl00_ContentPlaceHolder1_spnStkData')
                table_vals = stockdata.find('table').findAll('tr')
                if datetime.today().date() == dateObj.date():
                    row = table_vals[2]
                    tds = row('td')
                    if not (tds[0].string == dateObj.strftime(u'%-d/%m/%y')):
                        print ("{code} No data available for today".format(code=stock.security))
                        return False
                elif datetime.today().date() > dateObj.date():
                    #print ("Look for old date {date}".format(date = day.strftime(u'%-d/%m/%y')))
                    for row in table_vals:
                        tds = row('td')
                        if(tds[0].string == dateObj.strftime(u'%-d/%m/%y')):
                            break
                        else:
                            tds = None
                else:
                    raise Exception('Cannot fetch data for future')

                if tds is not None:
                    listing = Listing()
                    listing.stock = stock
                    listing.date = datetime.strptime(tds[0].string, "%d/%m/%y")
                    listing.opening = float(tds[1].string.replace(',', ''))
                    listing.high = float(tds[2].string.replace(',', ''))
                    listing.low = float(tds[3].string.replace(',', ''))
                    listing.closing = float(tds[4].string.replace(',', ''))
                    listing.wap = float(tds[5].string.replace(',', ''))
                    listing.traded = float(tds[6].string.replace(',', ''))
                    listing.trades = float(tds[7].string.replace(',', ''))
                    listing.turnover = float(tds[8].string.replace(',', ''))
                    listing.deliverable = float(tds[9].string.replace(',', ''))
                    listing.ratio = float(tds[10].string.replace(',', ''))
                    listing.spread_high_low = float(tds[11].string.replace(',', ''))
                    listing.spread_close_open = float(tds[12].string.replace(',', ''))
                    listing.save()
                    print ("{code} OK[{d}] [{id}]".format(code=stock.security, d=listing.date, id=listing.id))
                    return True
                else:
                    print ("{code} No data available for the day".format(code=stock.security))
                    return False
            except:
                print ("{code} Failed".format(code=stock.security))
                le = len(sys.exc_info())
                print(le)
                for i in range(0,le):
                    print("Unexpected error:", sys.exc_info()[i])
                return False
    except:
        print ("{code} Failed".format(code=stock.security))
        le = len(sys.exc_info())
        print(le)
        for i in range(0,le):
            print("Unexpected error:", sys.exc_info()[i])
        return False
      
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

def get_next_stock():
	try:
		stocks = Stock.objects.all()
		for stock in stocks:
			yield stock
	except:
		print ("Error in retreiving stock list and iterating")


def work_loop(thread_name, tid):
    global stock_iter
    for stock in stock_iter:
        get_data_for_date(stock, day)


day = datetime.today()
if len(sys.argv) == 2:
  day = datetime.datetime.strptime(sys.argv[1], "%d/%m/%y")

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
  print ("Unable to start thread ({id})".format(id=thread_id))

