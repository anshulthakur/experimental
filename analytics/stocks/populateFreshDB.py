import os
import sys
import init
import csv
from datetime import datetime
import dateparser

scrip_list = 'scrip_list.csv'
from stocks.models import Listing, Industry, Stock, Market, Company

def legacy():
    def populate_trades(stock):
        try:
            #print(stock.symbol)
            fd = open('bsedata/{name}.csv'.format(name=stock.symbol), 'r')
            reader = csv.DictReader(fd,delimiter=",", quotechar="'")
            #print(stock)
            listings = []
            for row in reader:
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
            try:
                Listing.objects.bulk_create(listings)
            except Exception as e:
                print(e)
                print(("Unexpected error:", sys.exc_info()[0]))
        except IOError:
            pass

    #Industries
    fd = open(scrip_list, 'r')
    reader = csv.DictReader(fd)
    for row in reader:
        industry = row.get('Industry', '').strip()
        if industry is not None and len(industry) > 0:
            try:
                Industry.objects.get(name=industry)
            except Industry.DoesNotExist:
                #print('Creating {}'.format(industry))
                Industry(name=industry).save()
            except Exception as e:
                print(e)
                print(("Unexpected error:", sys.exc_info()[0]))

    fd.close()

    print((len(Industry.objects.all())))

    #Markets
    markets = ['NSE', 'BSE']
    for market in markets:
        mkt = market
        try:
            Market.objects.get(name=market)
        except Market.DoesNotExist:
            #print('Creating {}'.format(industry))
            Market(name=market).save()
        except Exception as e:
            print(e)
            print(("Unexpected error:", sys.exc_info()[0]))


    #List of Equity
    fd = open(scrip_list, 'r')
    reader = csv.DictReader(fd)

    stocks = []
    for row in reader:
        #print(row)
        try:
            if row.get('Status','') != 'Active':
                continue
            market = Market.objects.get(name = 'BSE')
            industry = row.get('Industry', '').strip()
            if industry is not None and len(industry) > 0:
                industry = Industry.objects.get(name=industry)
            else:
                industry = None
            stock = Stock(symbol=int(row.get('Security Code')),
                sid = row.get('Security Id'),
                name = row.get('Security Name').strip(),
                group = row.get('Group').strip(),
                face_value = row.get('Face Value').strip(),
                isin = row.get('ISIN No').strip(),
                industry = industry,
                market = market)
            #stocks += [stock]
            #print(stock.name)
            stock.save()
            #populate_trades(stock)
        except Exception as e:
            print(stock)
            print(e)
            print(("Unexpected error:", sys.exc_info()[0]))

    fd.close()
    if len(stocks)>0:
        Stock.objects.bulk_create(stocks)


    #Historical Trading Data
    print((len(Stock.objects.all())))

    #fd = open('ListOfScrips.csv', 'r')
    fd = open(scrip_list, 'r')
    reader = csv.DictReader(fd)
    sids = []
    for row in reader:
        if row.get('Status','') == 'Active':
            sids += [row.get('Security Code')]

    fd.close()

    for sid in sids:
        #print(row)
        try:
            stock = Stock.objects.get(symbol=int(sid))
            populate_trades(stock)
        except:
            print(stock)
            print(("Unexpected error:", sys.exc_info()[0]))

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
    return data

def parse_bse_bhav(reader, symbols, fname):
    listings = []
    deliveries = None
    dateval = datetime.strptime(fname.upper().replace('EQ','').replace('.CSV',''), '%d%m%y')
    #print(symbols)
    for row in reader:
        if row.get('SC_CODE', None) is not None:
            if row.get('SC_CODE').strip() not in symbols:
                #print(f"{row.get('SC_CODE')}({row.get('SC_NAME')}) has not been added to DB yet. Skip.")
                continue
            if deliveries is None:
                deliveries = parse_bse_delivery(dateval)
            listing = Listing(date=dateval,
                              open=row.get('OPEN'),
                              high=row.get('HIGH'),
                              low=row.get('LOW'),
                              close=row.get('CLOSE'),
                              traded=row.get('NO_OF_SHRS'),
                              trades=row.get('NO_TRADES'),
                              stock = symbols[row.get('SC_CODE').strip()])
            if row.get('SC_CODE').strip() in deliveries:
                listing.deliverable = deliveries.get(row.get('SC_CODE'))
            listings.append(listing)
    try:
        if len(listings)>0:
            Listing.objects.bulk_create(listings)
    except Exception as e:
        print(e)
        print(("Unexpected error:", sys.exc_info()[0]))

def parse_nse_delivery(dateval):
    data = {}
    filename = 'MTO_{}.csv'.format(dateval.strftime('%d%m%Y'))
    path = MARKET_DATA['NSE']['delivery']+filename
    print('Parsing '+path)
    with open(path,'r') as fd:
        reader = csv.DictReader(fd)
        for row in reader:
            data[row['Name of Security']]= row['Deliverable Quantity']
    return data

def parse_nse_bhav(reader, symbols, fname):
    listings = []
    deliveries = None
    #print(symbols)
    for row in reader:
        if row.get('SYMBOL', None) is not None:
            #print(row)
            if row.get('SYMBOL') not in symbols:
                #print(f"{row.get('SYMBOL')} has not been added to DB yet. Skip.")
                continue
            if deliveries is None:
                deliveries = parse_nse_delivery(dateparser.parse(row.get('TIMESTAMP').strip()))
            listing = Listing(date=dateparser.parse(row.get('TIMESTAMP').strip()),
                              open=row.get('OPEN'),
                              high=row.get('HIGH'),
                              low=row.get('LOW'),
                              close=row.get('CLOSE'),
                              traded=row.get('TOTTRDQTY'),
                              stock = symbols[row.get('SYMBOL')])
            if row.get('SYMBOL') in deliveries:
                listing.deliverable = deliveries.get(row.get('SYMBOL'))
            listings.append(listing)
    try:
        if len(listings)>0:
            Listing.objects.bulk_create(listings)
    except Exception as e:
        print(e)
        print(("Unexpected error:", sys.exc_info()[0]))

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
    import argparse
    parser = argparse.ArgumentParser(description='Download stock data for stock/date')
    parser.add_argument('-m', '--market', help="Market (NSE/BSE/MCX/...)")
    args = parser.parse_args()
    stock_code = None
    market = None

    if args.market is not None and len(args.market)>0:
        market = args.market

    if market is not None and market not in ['BSE', 'NSE']:
        print(f'{market} not supported currently')
    elif market is not None:
        populate_db(market)
    else:
        populate_db(market='NSE')
        populate_db(market='BSE')