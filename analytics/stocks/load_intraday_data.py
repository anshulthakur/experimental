'''
Created on 13-Mar-2022

@author: Anshul
@brief Load up intraday data into the DB for Nifty and Banknifty for analysis purposes
'''

import os
import sys
import init
import csv
from datetime import datetime

from stocks.models import Listing, Industry, Stock

def load_file_list(directory="./intraday/Consolidated"):
    file_list = []
    for filename in os.listdir(directory):
        f = os.path.join(directory, filename)
        # checking if it is a file
        if os.path.isfile(f) and f.endswith('.txt'):
            file_list.append(f)
    return file_list

def load_intraday_data_to_db(first_time=False):
    filelist = load_file_list()
    stock = None
    for f in filelist:
        print('Reading: {}'.format(f))
        with open(f, 'r') as fd:
            reader = csv.DictReader(fd, 
                                  fieldnames=['instrument', 'date', 'time', 'open', 'high', 'low', 'close'], 
                                  delimiter=",", quotechar="'")
            listings = []
            for row in reader:
                try:
                    if stock is None:
                        stock = Stock.objects.get(name=row.get('instrument'))
                    if first_time:
                        listing  = Listing(stock=stock,
                                  date = datetime.strptime(row.get('date')+' '+row.get('time'), 
                                                           '%Y%m%d %H:%M'),
                                  opening=float(row.get('open').strip()),
                                  high = float(row.get('high').strip()),
                                  low = float(row.get('low').strip()),
                                  close = float(row.get('close').strip()),
                                  wap = 0,
                                  traded = 0,
                                  trades = 0,
                                  turnover = 0,
                                  deliverable = 0)
                        try:
                            listings.append(listing)
                        except:
                            print(row)
                            print(listing)
                            for e in sys.exc_info():
                                print(("Unexpected error:", e))
                    else:
                        listing = Listing.objects.get(stock=stock, date = datetime.strptime(row.get('date')+' '+row.get('time'), 
                                                                                        '%Y%m%d %H:%M'))
                except Stock.DoesNotExist:
                    print(f'Entry for {row.get("instrument")} not found.')
                    break
                except Listing.DoesNotExist:
                    listing  = Listing(stock=stock,
                                  date = datetime.strptime(row.get('date')+' '+row.get('time'), 
                                                           '%Y%m%d %H:%M'),
                                  opening=float(row.get('open').strip()),
                                  high = float(row.get('high').strip()),
                                  low = float(row.get('low').strip()),
                                  close = float(row.get('close').strip()),
                                  wap = 0,
                                  traded = 0,
                                  trades = 0,
                                  turnover = 0,
                                  deliverable = 0)
                    try:
                        listings.append(listing)
                    except:
                        print(row)
                        print(listing)
                        for e in sys.exc_info():
                            print(("Unexpected error:", e))
                except:
                    print(("Unexpected error:", sys.exc_info()[0]))
            try:
                Listing.objects.bulk_create(listings)
            except:
                print(("Unexpected error:", sys.exc_info()[0]))

if __name__ == "__main__":
    #Try and create symbols for Nifty and BankNifty (cooked)
    stock = Stock(security=int(0),
              sid = '0',
              name = 'NIFTY',
              group = 'INDEX',
              face_value = 0,
              isin = '0',
              industry = None)
    stock.save()
    stock = Stock(security=int(1),
              sid = '1',
              name = 'BANKNIFTY',
              group = 'INDEX',
              face_value = 0,
              isin = '0',
              industry = None)
    stock.save()
    load_intraday_data_to_db(first_time=True)