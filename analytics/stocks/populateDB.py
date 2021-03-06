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
            #print(stock)
            #print(row)
            try:
                listing = Listing.objects.get(stock=stock, date=datetime.strptime(row.get('Date'), '%d-%B-%Y'))
            except Listing.DoesNotExist:
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
                try:
                    #listing.save()
                    #print('Adding to list')
                    listings.append(listing)
                except:
                    print(stock)
                    print(row)
                    print(listing)
                    for e in sys.exc_info():
                        print(("Unexpected error:", e))
            except:
                print('Something went wrong')
                print(stock)
                print(row)
                print(("Unexpected error:", sys.exc_info()[0]))
        try:
            Listing.objects.bulk_create(listings)
        except:
            print(("Unexpected error:", sys.exc_info()[0]))
    except IOError:
        pass

#Industries
#fd = open('industries.txt', 'r')
fd = open('ListOfScrips_equity.csv', 'r')
reader = csv.DictReader(fd)
for row in reader:
    industry = row.get('Industry', '').strip()
    if industry is not None and len(industry) > 0:
        try:
            Industry.objects.get(name=industry)
        except Industry.DoesNotExist:
            Industry(name=industry).save()
        except Exception as e:
            print(e)
            print(("Unexpected error:", sys.exc_info()[0]))

Industry.objects.bulk_create(stocks)
fd.close()

print((len(Industry.objects.all())))

#List of Equity


#fd = open('ListOfScrips.csv', 'r')
fd = open('ListOfScrips_equity.csv', 'r')
reader = csv.DictReader(fd)


for row in reader:
    #print(row)
    try:
        stock = Stock.objects.get(security=int(row.get('Security Code')))
        #print(stock)
        populate_trades(stock)
    except Stock.DoesNotExist:
        industry = row.get('Industry', '').strip()
        try:
          if industry is not None and len(industry) > 0:
              industry = Industry.objects.get(name=industry)
          else:
              industry = None
        except Industry.DoesNotExist:
            print('Industry: {} does not exist'.format(row.get('Industry', '').strip()))
            exit()
        stock = Stock(security=int(row.get('Security Code')),
              sid = row.get('Security Id'),
              name = row.get('Security Name').strip(),
              group = row.get('Group').strip(),
              face_value = row.get('Face Value').strip(),
              isin = row.get('ISIN No').strip(),
              industry = industry)
        stock.save()
        print(stock)
        populate_trades(stock)
    except:
            print(("Unexpected error:", sys.exc_info()[0]))

fd.close()

print((len(Stock.objects.all())))

