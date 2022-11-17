import os
import sys
import settings
import csv
from datetime import datetime
import dateparser

from stocks.models import Listing, Industry, Stock, Market, Company

def legacy():
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

MARKET_DATA = {
                'NSE': {'bhav': './nseData/',
                       'delivery': './nseData/delivery/',
                        },
                'BSE': {'bhav': './bseData/',
                       'delivery': './bseData/delivery/',
                        }
               }

def get_filelist(folder):
    files = os.listdir(folder)
    files = [f for f in files if os.path.isfile(folder+'/'+f) and f[-3:].strip().lower()=='csv'] #Filtering only the files.
    return files

def parse_bse_delivery(dateval):
    data = {}
    filename = 'SCBSEALL{}.csv'.format(dateval.strftime('%d%m'))
    path = MARKET_DATA['BSE']['delivery']+'{year}/'.format(year=dateval.date().year)+filename
    print('Parsing '+path)

    try:
        try:
            with open(path,'r') as fd:
                reader = csv.DictReader(fd)
                for row in reader:
                    data[row['SCRIP CODE'].strip()]= row['DELIVERY QTY']
        except FileNotFoundError:
            filename = 'scbseall{}.csv'.format(dateval.strftime('%d%m'))
            path = MARKET_DATA['BSE']['delivery']+'{year}/'.format(year=dateval.date().year)+filename
            with open(path,'r') as fd:
                reader = csv.DictReader(fd)
                for row in reader:
                    data[row['SCRIP CODE'].strip()]= row['DELIVERY QTY']
    except:
        print('Error parsing delivery file {}'.format(filename))
    return data

def parse_bse_bhav(reader, symbols, fname):
    deliveries = None
    dateval = datetime.strptime(fname.upper().replace('EQ','').replace('.CSV',''), '%d%m%y')
    market = Market.objects.get(name='BSE')
    #print(symbols)
    for row in reader:
        if row.get('SC_CODE', None) is not None:
            if row.get('SC_CODE').strip() not in symbols:
                #print(f"{row.get('SC_CODE')}({row.get('SC_NAME')}) has not been added to DB yet. Skip.")
                continue
            if deliveries is None:
                deliveries = parse_bse_delivery(dateval)
            stock = Stock.objects.get(sid=row.get('SC_CODE'),
                                        market=market)
            try:
                #listing = Listing.objects.filter(stock=stock, date__contains = dateval)
                #if len(listing) == 0:
                listing = Listing.objects.get(stock=stock, date = dateval)
                if listing.deliverable is None and row.get('SC_CODE') in deliveries:
                    listing.deliverable = deliveries.get(row.get('SC_CODE'))
                    print('Update delivery data')
                    listing.save()
            except Listing.DoesNotExist:
                #print('Create entry')
                listing = Listing(date=dateval,
                                  open=row.get('OPEN'),
                                  high=row.get('HIGH'),
                                  low=row.get('LOW'),
                                  close=row.get('CLOSE'),
                                  traded=row.get('NO_OF_SHRS'),
                                  trades=row.get('NO_TRADES'),
                                  stock = stock)
                if row.get('SC_CODE') in deliveries:
                    listing.deliverable = deliveries.get(row.get('SC_CODE'))
                    listing.save()
            except Exception as e:
                print(e)
                print(("Unexpected error:", sys.exc_info()[0]))
                listing = Listing.objects.filter(stock=stock, date__contains = dateval)
                for l in listing:
                    print(l)
                continue
            

def parse_nse_delivery(dateval):
    data = {}
    filename = 'MTO_{}.csv'.format(dateval.strftime('%d%m%Y'))
    path = MARKET_DATA['NSE']['delivery']+filename
    print('Parsing '+path)
    try:
        with open(path,'r') as fd:
            reader = csv.DictReader(fd)
            for row in reader:
                data[row['Name of Security']]= row['Deliverable Quantity']
    except:
        print('Error parsing {}'.format(filename))
    return data

def parse_nse_bhav(reader, symbols, fname):
    deliveries = None
    #print(symbols)
    market = Market.objects.get(name='NSE')
    dateval = None
    for row in reader:
        if row.get('SYMBOL', None) is not None:
            #print(row)
            if row.get('SYMBOL') not in symbols:
                #print(f"{row.get('SYMBOL')} has not been added to DB yet. Skip.")
                continue
            if dateval is None:
                dateval = dateparser.parse(row.get('TIMESTAMP').strip())
            if deliveries is None:
                deliveries = parse_nse_delivery(dateval)
            stock = Stock.objects.get(symbol=row.get('SYMBOL'),
                                        market=market)
            try:
                #listing = Listing.objects.filter(stock=stock, date__contains = dateval)
                #if len(listing) == 0:
                listing = Listing.objects.get(stock=stock, date = dateval)
                if listing.deliverable is None and row.get('SYMBOL') in deliveries:
                    listing.deliverable = deliveries.get(row.get('SYMBOL'))
                    print('Update delivery data')
                    listing.save()
            except Listing.DoesNotExist:
                #print('Create entry')
                listing = Listing(date=dateval,
                                open=row.get('OPEN'),
                                high=row.get('HIGH'),
                                low=row.get('LOW'),
                                close=row.get('CLOSE'),
                                traded=row.get('TOTTRDQTY'),
                                stock = stock)
                if row.get('SYMBOL') in deliveries:
                    listing.deliverable = deliveries.get(row.get('SYMBOL'))
                    listing.save()
            except Exception as e:
                print(e)
                print(("Unexpected error:", sys.exc_info()[0]))
                listing = Listing.objects.filter(stock=stock, date__contains = dateval)
                for l in listing:
                    print(l)
                continue
            

def get_active_scrips(market):
    symbols = {}
    if market.name=='NSE':
        from nseDownload import get_scrip_list
        members = get_scrip_list(offline=True)
    elif market.name=='BSE':
        from bseDownload import get_scrip_list
        members = get_scrip_list()
    for member in members:
        try:
            company = Company.objects.get(isin=member.get('isin'))
        except Company.DoesNotExist:
            company = Company(isin=member.get('isin'),
                                name=member.get('name'))
            company.save()
        try:
            stock = Stock.objects.get(symbol=member.get('symbol'), market=market)
        except Stock.DoesNotExist:
            stock = Stock(face_value=int(float(member.get('facevalue'))),
                          market = market,
                          symbol=member.get('symbol'),
                          content_object=company
                         )
            if market.name=='BSE':
                stock.sid = member.get('security')
            stock.save()
        if market.name=='NSE':
            symbols[member.get('symbol')] = stock
        elif market.name=='BSE':
            symbols[member.get('security')] = stock
    return symbols

def get_bhav_filename(day, market):
    months = ['', 'JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 
                  'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
    fname = None
    if market=='BSE':
        fname= 'EQ{day:02}{month:02}{year}.csv'.format(day = day.day, 
                                                        month = day.month, 
                                                        year = day.year)
    elif market=='NSE':
        fname='cm{day:02}{month}{year:04}bhav.csv'.format(day = day.day, 
                                                        month = months[day.month], 
                                                        year = day.year)
    return fname

def populate_db_1(market, day=datetime.today(), bulk=False):
    #Create Market object
    market_obj = None
    try:
        market_obj = Market.objects.get(name=market)
    except Market.DoesNotExist:
        print(f'Create for {market}')
        market_obj = Market(name=market)
        market_obj.save()
    except Exception as e:
        print(e)
        print(("Unexpected error:", sys.exc_info()[0]))
        return
    #Create scrip members (and Company if applicable)
    symbols = get_active_scrips(market_obj)

    #Handle bhav data
    print('Parsing '+MARKET_DATA[market]['bhav']+get_bhav_filename(day=day, market=market))    
    with open(MARKET_DATA[market]['bhav']+get_bhav_filename(day=day, market=market),'r') as fd:
        reader = csv.DictReader(fd)
        if market=='NSE':
            parse_nse_bhav(reader, symbols, f)
        elif market=='BSE':
            parse_bse_bhav(reader, symbols, f)

def populate_db(market):
    #Create Market object
    market_obj = None
    try:
        market_obj = Market.objects.get(name=market)
    except Market.DoesNotExist:
        print(f'Create for {market}')
        market_obj = Market(name=market)
        market_obj.save()
    except Exception as e:
        print(e)
        print(("Unexpected error:", sys.exc_info()[0]))
        return
    #Create scrip members (and Company if applicable)
    print(market_obj)
    symbols = get_active_scrips(market_obj)

    #Handle bhav data
    files = get_filelist(MARKET_DATA[market]['bhav'])
    for f in files:
        print('Parsing '+MARKET_DATA[market]['bhav']+f)
        with open(MARKET_DATA[market]['bhav']+f,'r') as fd:
            reader = csv.DictReader(fd)
            if market=='NSE':
                parse_nse_bhav(reader, symbols, f)
            elif market=='BSE':
                parse_bse_bhav(reader, symbols, f)


if __name__ == "__main__":
    day = datetime.today()
    bulk = False
    import argparse
    parser = argparse.ArgumentParser(description='Download stock data for stock/date')
    parser.add_argument('-m', '--market', help="Market (NSE/BSE/MCX/...)")
    parser.add_argument('-d', '--date', help="Date")
    parser.add_argument('-b', '--bulk', help="Get bulk data for stock(s)", action="store_true", default=False)

    args = parser.parse_args()
    stock_code = None
    market = None

    if args.date is not None and len(args.date)>0:
        print('Get data for date: {}'.format(args.date))
        day = datetime.strptime(args.date, "%d/%m/%y")

    if args.market is not None and len(args.market)>0:
        market = args.market

    if args.bulk:
        bulk = True

    if market is not None and market not in ['BSE', 'NSE']:
        print(f'{market} not supported currently')
    elif market is not None:
        populate_db(market)
    else:
        populate_db(market='NSE')
        populate_db(market='BSE')