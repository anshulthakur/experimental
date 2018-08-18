import os
import sys
import settings
import csv
from datetime import datetime

from stocks.models import Listing, Industry, Stock

def populate_trades(stock):
    try:
        #print(stock.security)
        fd = open('bsedata/{name}.csv'.format(name=stock.security), 'r')
        reader = csv.DictReader(fd,delimiter=",", quotechar="'")
        #print(stock)
        listings = []
        for row in reader:
            listing = Listing(stock=stock,
                                  date=datetime.strptime(row.get('Date').strip(), '%d-%B-%Y'),
                                  opening=float(row.get('Open Price').strip()),
                                  high = float(row.get('High Price').strip()),
                                  low = float(row.get('Low Price').strip()),
                                  closing = float(row.get('Close Price').strip()),
                                  wap = float(row.get('WAP').strip()),
                                  traded = int(row.get('No.of Shares').strip()),
                                  trades = int(row.get('No. of Trades').strip()),
                                  turnover = float(row.get('Total Turnover (Rs.)').strip()),
                                  deliverable = float(row.get('Deliverable Quantity').strip()) if len(row.get('Deliverable Quantity').strip()) > 0 else 0,
                                  ratio = float(row.get('% Deli. Qty to Traded Qty').strip()) if len(row.get('% Deli. Qty to Traded Qty').strip()) > 0 else 0,
                                  spread_high_low = float(row.get('Spread High-Low').strip()),
                                  spread_close_open = float(row.get('Spread Close-Open').strip()))
            listings += [listing]
        try:
            Listing.objects.bulk_create(listings) 
        except:
            print("Unexpected error:", sys.exc_info()[0])
    except IOError:
        pass

#Industries
fd = open('industries.txt', 'r')
for line in fd:
    industry = line.strip()
    if len(industry) > 0:
        try:
            Industry.objects.get(name=industry)
        except Industry.DoesNotExist:
            Industry(name=industry).save()
        except:
            print("Unexpected error:", sys.exc_info()[0])

fd.close()

print(len(Industry.objects.all()))

#List of Equity
fd = open('ListOfScrips.csv', 'r')
reader = csv.DictReader(fd)

stocks = []
for row in reader:
    #print(row)
    try:
        industry = row.get('Industry', '').strip()
        if industry is not None and len(industry) > 0:
            industry = Industry.objects.get(name=industry)
        else:
            industry = None
        stock = Stock(security=int(row.get('Security Code')),
              sid = row.get('Security Id'),
              name = row.get('Security Name').strip(),
              group = row.get('Group').strip(),
              face_value = row.get('Face Value').strip(),
              isin = row.get('ISIN No').strip(),
              industry = industry)
        stocks += [stock]
        #populate_trades(stock)
    except:
        print(stock)
        print("Unexpected error:", sys.exc_info()[0])

fd.close()
Stock.objects.bulk_create(stocks)

#Historical Trading Data
print(len(Stock.objects.all()))

fd = open('ListOfScrips.csv', 'r')
reader = csv.DictReader(fd)
sids = []
for row in reader:
    sids += [row.get('Security Code')]

fd.close()

for sid in sids:
    #print(row)
    try:
        stock = Stock.objects.get(security=int(sid))
        populate_trades(stock)
    except:
        print(stock)
        print("Unexpected error:", sys.exc_info()[0])



