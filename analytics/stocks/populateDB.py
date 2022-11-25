import os
import sys
import settings
import csv
from datetime import datetime, timedelta
import dateparser
import signal

from stocks.models import Listing, Industry, Stock, Market, Company

from nseDownload import download_archive as download_nse_data
from bseDownload import download_archive as download_bse_data
error_dates = []
MARKET_DATA = {
                'NSE': {'bhav': './nseData/',
                       'delivery': './nseData/delivery/',
                        },
                'BSE': {'bhav': './bseData/',
                       'delivery': './bseData/delivery/',
                        }
               }

def write_error_file():
    global error_dates
    with open("error_dates.txt", 'a') as fd:
        for e in error_dates:
            fd.write(e+'\n')

def handler(signum, frame):
    write_error_file()
    print('Error dates appended to error_dates.txt')
    exit(0)

def get_filelist(folder):
    files = os.listdir(folder)
    files = [f for f in files if os.path.isfile(folder+'/'+f) and f[-3:].strip().lower()=='csv'] #Filtering only the files.
    return files

def parse_bse_delivery(dateval):
    global error_dates
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
    global error_dates
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
                print('Create entry for {}'.format(stock.symbol))
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
                print('Create entry for {}'.format(stock.symbol))
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
        fname= 'EQ{day:02}{month:02}{year}.CSV'.format(day = day.day, 
                                                        month = day.month, 
                                                        year = str(day.year)[-2:])
    elif market=='NSE':
        fname='cm{day:02}{month}{year:04}bhav.csv'.format(day = day.day, 
                                                        month = months[day.month], 
                                                        year = day.year)
    return fname


def populate_for_date(market, symbols, date=datetime.today()):
    global error_dates
    f = get_bhav_filename(day=date, market = market.name)
    print('Parsing '+MARKET_DATA[market.name]['bhav']+f)
    try:
        with open(MARKET_DATA[market.name]['bhav']+f,'r') as fd:
            reader = csv.DictReader(fd)
            if market.name=='NSE':
                parse_nse_bhav(reader, symbols, f)
            elif market.name=='BSE':
                parse_bse_bhav(reader, symbols, f)
    except FileNotFoundError:
        if date.weekday()<=4: #Weekends won't have a file
            error_dates.append(date.strftime('%d-%m-%y'))
            print('Error')
        pass

def populate_db(market, date = None, bulk=False):
    #Create Market object
    if date is None and bulk is False:
        date = datetime.today()
    elif date is None and bulk is True:
        date = datetime.strptime('01-01-2010', "%d-%m-%Y").date()
    
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
    if bulk:
        while date <= datetime.today().date():
            populate_for_date(market = market_obj, symbols=symbols, date=date)
            date = date + timedelta(days=1)
    else:
        populate_for_date(market = market_obj, symbols=symbols, date=date)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, handler)
    import argparse
    parser = argparse.ArgumentParser(description='Download stock data for stock/date')
    parser.add_argument('-m', '--market', help="Market (NSE/BSE/MCX/...)")
    parser.add_argument('-d', '--date', help="Date")
    parser.add_argument('-b', '--bulk', help="Get bulk data for stock(s)", action="store_true", default=False)

    args = parser.parse_args()
    market = None
    day = None

    if args.date is not None and len(args.date)>0:
        print('Get data for date: {}'.format(args.date))
        day = datetime.strptime(args.date, "%d/%m/%y").date()

    if args.market is not None and len(args.market)>0:
        market = args.market

    if market is not None:
        if market not in ['BSE', 'NSE']:
            print(f'{market} not supported currently')
            exit(0)
        elif market=='NSE':
            download_nse_data(day, args.bulk)
        elif market=='BSE':
            download_bse_data(day, args.bulk)
        populate_db(market, date=day, bulk=args.bulk)
    else:
        download_nse_data(day, args.bulk)
        download_bse_data(day, args.bulk)
        populate_db(market='NSE', date=day, bulk=args.bulk)
        populate_db(market='BSE', date=day, bulk=args.bulk)
    write_error_file()
