from bs4 import BeautifulSoup
import sys
import os

import csv
import datetime

import threading
import multiprocessing
import urllib

download_dir = "/home/craft/tests/experimental/analytics/stocks/bsedata/"

day = datetime.date.today()

num_threads = 8

processed = []
failed = []
skipped = []

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


class CompanyArchivedQuotes:
  def __init__(self, company_code):
    #company_code is the scrip code of the company found in BSEIndian website
    self.company_code = company_code
    #URL Link to download stock quotes of a company from BSEIndia website
    self.url = "http://www.bseindia.com/markets/equity/EQReports/StockPrcHistori.aspx?expandable=7&scripcode=" + company_code + "&flag=sp&Submit=G"
    self.company_name = ""
    self.date = []
    self.open_price = []
    self.day_high = []
    self.day_low = []
    self.close_price = []
    self.wap = []
    self.no_of_shares = []
    self.no_of_trades = []
    self.total_turnover = []
    self.deliverable_qty = []
    self.percent_qty = []
    self.highlow_spread = []
    self.closeopen_spread = []
    self.read_fd = open('bsedata/'+self.company_code+'.csv', 'r')
    #Parse the URL to get the various stock attributes values
    if self.write_needed(self.read_fd) is False:
      self.ok = True
    else :
      self.ok = self.parse_URL()
      if self.ok:
        self.write_csv()
    self.read_fd.close()


  #function to parse stock data from URL
  def parse_URL(self):
    global current
    global day
    #open and read from URL
    try:
      response = urllib.urlopen(self.url)
      html_page = response.read()
      soup = BeautifulSoup(html_page, "html.parser")
      self.company_name = soup.find('span', id='ctl00_ContentPlaceHolder1_lblCompanyValue').find('a').string
      stockdata = soup.find('span', id='ctl00_ContentPlaceHolder1_spnStkData')
      table_vals = stockdata.find('table').findAll('tr')
      if current:
        row = table_vals[2]
        tds = row('td')
        if not (tds[0].string == day.strftime(u'%-d/%m/%y')):
          print "{code} No data available for today".format(code=self.company_code)
          return False
      else:
        #print "Look for old date {date}".format(date = day.strftime(u'%-d/%m/%y'))
        for row in table_vals:
          tds = row('td')
          if(tds[0].string == day.strftime(u'%-d/%m/%y')):
            break
          else:
            tds = None

      if tds is not None:
        self.date = datetime.datetime.strptime(tds[0].string, "%d/%m/%y")
        self.date = self.date.strftime('%d-%B-%Y')
        self.open_price = float(tds[1].string.replace(',', ''))
        self.day_high = float(tds[2].string.replace(',', ''))
        self.day_low = float(tds[3].string.replace(',', ''))
        self.close_price = float(tds[4].string.replace(',', ''))
        self.wap = float(tds[5].string.replace(',', ''))
        self.no_of_shares = float(tds[6].string.replace(',', ''))
        self.no_of_trades = float(tds[7].string.replace(',', ''))
        self.total_turnover = float(tds[8].string.replace(',', ''))
        self.deliverable_qty = float(tds[9].string.replace(',', ''))
        self.percent_qty = float(tds[10].string.replace(',', ''))
        self.highlow_spread = float(tds[11].string.replace(',', ''))
        self.closeopen_spread = float(tds[12].string.replace(',', ''))
        print "{code} OK".format(code=self.company_code)
        return True
      else:
        print "{code} No data available for the day".format(code=self.company_code)
        return False
    except:
      print "{code} Failed".format(code=self.company_code)
      return False

  def write_needed(self, fd):
    reader = csv.reader(fd,delimiter=",")
    for row in reversed(list(reader)):
      if row[0] == day.strftime('%d-%B-%Y'):
        print 'Skipping {code}'.format(code=self.company_code)
        return False
      else:
        return True

  def write_csv(self):
    fd = open('bsedata/'+self.company_code+'.csv', 'a')
  
    writer = csv.writer(fd)
    writer.writerow([self.date,
                    self.open_price,
                    self.day_high,
                    self.day_low,
                    self.close_price,
                    self.wap,
                    self.no_of_shares,
                    self.no_of_trades,
                    self.total_turnover,
                    self.deliverable_qty,
                    self.percent_qty,
                    self.highlow_spread,
                    self.closeopen_spread
                    ])
    fd.close()
  #print all the attributes as headers in a single line to aid in report creation
  @staticmethod
  def print_headers(delimiter = ','):
     print "Company" + delimiter \
     + "Date" + delimiter \
     + "Open Price" + delimiter \
     +"High Price" + delimiter \
     + "Low Price" + delimiter \
     + "Close Price" + delimiter\
     + "WAP" + delimiter \
     + "No. of shares" + delimiter \
     + "No. of trades" + delimiter \
     + "Total turnover" + delimiter \
     + "Deliverable Quantity" + delimiter \
     + "%Delivered Qty to Traded Qty" + delimiter \
     + "High-Low Spread" + delimiter \
     + "Close-Open Spread"
  #print all the stock attribute values in a single line separated by delimiter
  def print_info(self, delimiter = ','):
    for i in range(1, len(self.date)):
    	print self.company_name + delimiter \
              + self.date[i] + delimiter \
    	+ str(self.open_price[i]) + delimiter \
    	+ str(self.day_high[i]) + delimiter \
    	+ str(self.day_low[i]) + delimiter \
    	+ str(self.close_price[i]) + delimiter \
    	+ str(self.wap[i]) + delimiter \
    	+ str(self.no_of_shares[i]) + delimiter \
    	+ str(self.no_of_trades[i]) + delimiter \
    	+ str(self.total_turnover[i]) + delimiter \
    	+ str(self.deliverable_qty[i]) + delimiter \
    	+ str(self.percent_qty[i]) + delimiter \
    	+ str(self.highlow_spread[i]) + delimiter \
    	+ str(self.closeopen_spread[i])


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

#For thread safe operation using locks      
class LockedIterator(object):
    def __init__(self, it):
        self.lock = threading.Lock()
        self.it = it.__iter__()

    def __iter__(self): return self

    def next(self):
        self.lock.acquire()
        try:
            return self.it.next()
        finally:
            self.lock.release()


def work_loop(thread_name, tid):
  global stock_codes
  for stock in stock_codes:
    if os.path.exists(download_dir+"{file_name}".format(file_name=stock.code)+".csv"):
      obj = CompanyArchivedQuotes(str(stock.code))
      if obj.ok:
        processed.append(str(stock.code))
      else:
        failed.append(str(stock.code))

if len(sys.argv) < 2:
  print "Insufficient Parameters"
  print "Usage: %s <csv file of equity list>" %sys.argv[0]
  exit()
if len(sys.argv) == 3:
  day = datetime.datetime.strptime(sys.argv[2], "%d/%m/%y")


if day==datetime.date.today():
  current = True
else:
  current = False
stock_codes = LockedIterator(get_next_stock_code(sys.argv[1]))

threads = []
try:
  for thread_id in range(num_threads):
      t = threading.Thread(target=work_loop, args=("Thread ID {id}".format(id=thread_id), thread_id))
      threads.append(t)
      t.start()

  for t in threads:
      t.join()
except:
  print "Unable to start thread ({id})".format(id=thread_id)

failed_items = ','.join(failed)
retry_item_file = open(day.strftime('%d-%B-%Y')+'_failed.txt', 'w')
retry_item_file.write(failed_items)
retry_item_file.close()
