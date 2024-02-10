#In this exercise, we'll create our own indices comprising of a bucket of stocks and have 
# them weighted by their respective market capitalization. 
#
# I'm currently looking at creating sectoral indices and tracking their performance and seeing
# sector rotation

import os
import sys
import init
import datetime

import numpy as np
import pandas as pd
from django_pandas.io import read_frame
import pandas_datareader.data as pdr


import matplotlib
import matplotlib.pyplot as plt
from mplfinance.original_flavor import candlestick_ohlc
import matplotlib.dates as mpl_dates
from matplotlib.dates import date2num

# imports
from lib.tradingview import Interval, convert_timeframe_to_quant, get_tvfeed_instance
from lib.cache import cached
from lib.logging import set_loglevel, log
from lib.misc import create_directory
from lib.nse import NseIndia

#Prepare to load stock data as pandas dataframe from source. In this case, prepare django
import django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'settings')
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from stocks.models import Listing, Stock, Market
from lib.retrieval import get_stock_listing

import csv 
import json
nse_list = 'NSE_list.csv'
data_dir = './reports/details'
mcap_file = './reports/details/mcap_nse_dec23.csv'
indices_dir = './reports/members'
index_map = './reports/members/custom_indices.json'

def fetch_equity_industry_data():
    indices = []
    mcaps = {}
    with open(mcap_file, 'r') as fd:
        reader = csv.DictReader(fd)
        for row in reader:
            mcaps[row['Symbol'].strip().upper()] = int(row['MCAP'])
    with open(nse_list, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if row['SYMBOL'].strip().upper() in mcaps:
                indices.append(row['SYMBOL'].strip())
    
    create_directory(data_dir)
    nse = NseIndia()
    data = {}
    for scrip in indices:
        #print(scrip)
        if os.path.exists(f'{data_dir}/{scrip}.json'):
            with open(f'{data_dir}/{scrip}.json', 'r') as fd:
                detail = json.load(fd)
                data[scrip] = {'name': f'{detail["info"]["companyName"]}',
                               'industry': f'{detail.get("industryInfo")["industry"] if detail.get("industryInfo", None) is not None else "NA"}',
                               'sector': f'{detail["industryInfo"]["sector"] if detail.get("industryInfo", None) is not None else "NA"}',
                               'basicIndustry': f'{detail["industryInfo"]["basicIndustry"] if detail.get("industryInfo", None) is not None else "NA"}',
                               'mcap': mcaps.get(scrip)
                               }
            continue
        detail = json.loads(nse.getEquityDetails(scrip))
        #print(json.dumps(json.loads(detail), indent=2))
        with open(f'{data_dir}/{scrip}.json', 'w') as fd:
            fd.write(json.dumps(detail, indent=2))
        data[scrip] = {'name': f'{detail["info"]["companyName"]}',
                        'industry': f'{detail["industryInfo"]["industry"] if detail.get("industryInfo", None) is not None else "NA"}',
                        'sector': f'{detail["industryInfo"]["sector"] if detail.get("industryInfo", None) is not None else "NA"}',
                        'basicIndustry': f'{detail["industryInfo"]["basicIndustry"] if detail.get("industryInfo", None) is not None else "NA"}',
                        'mcap': mcaps.get(scrip)
                        }
    return data

def sanitize_name(name):
    name = name.replace(' - ', ' ')
    name = name.replace('-', '')
    name = name.replace(' ', '_')
    name = name.replace('/', '_')
    return name

def save_custom_index(name, constituents, data, prefix=''):
    '''
    Company Name	Industry	Symbol	Series	ISIN Code
    '''
    if len(name)==0 or name=='NA':
        return
    name = sanitize_name(name)
    with open(f'{indices_dir}/{prefix}{name}.csv', 'w') as fd:
        fd.write('Company Name,Industry,Symbol,Series,ISIN Code\n')
        for ii in range(0, len(constituents['members'])):
            scrip = constituents['members'][ii]
            fd.write(f'{data[scrip]["name"]},{data[scrip]["industry"]},{scrip},,{constituents["member_weight"][ii]}\n')

def get_index_dataframe(name, end_date, duration=300, sampling='w', online=True, path = None):
    members = {}
    interval = convert_timeframe_to_quant(sampling)
    if path is not None:
        with open(path, 'r') as fd:
            reader = csv.DictReader(fd)
            for row in reader:
                members[row['Symbol'].strip().upper()] = float(row['ISIN Code']) #ISIN Code field is overloaded with constitient weight
    else:
        with open(f'{indices_dir}/{sanitize_name(name)}.csv', 'r') as fd:
            reader = csv.DictReader(fd)
            for row in reader:
                members[row['Symbol'].strip().upper()] = float(row['ISIN Code']) #ISIN Code field is overloaded with constitient weight
    df_arr = []
    tv = None
    interval = convert_timeframe_to_quant(sampling)
    log(f'Samlping interval: {interval}', logtype='debug')
    username = 'AnshulBot'
    password = '@nshulthakur123'

    if online:
        tv = get_tvfeed_instance(username, password)

    for stock in members:
        try:
            if not online:
                market = Market.objects.get(name='NSE')
                stock_obj = Stock.objects.get(symbol=stock, market=market)
                s_df = get_stock_listing(stock_obj, duration=duration)
                s_df = s_df.drop(columns = ['open', 'high', 'low', 'volume', 'delivery', 'trades'])
                #print(s_df.head())
                if len(s_df)==0:
                    print('Skip {}'.format(stock_obj))
                    continue
                s_df.rename(columns={'close': stock},
                           inplace = True)
                s_df.reset_index(inplace = True)
                s_df['date'] = pd.to_datetime(s_df['date'], format='%d-%m-%Y')+ pd.Timedelta('9 hour') +  pd.Timedelta('15 minute')
                #s_df.drop_duplicates(inplace = True, subset='date')
                s_df.set_index('date', inplace = True)
                s_df = s_df.sort_index()
                s_df = s_df.reindex(columns = [stock])
                s_df = s_df[~s_df.index.duplicated(keep='first')]
                #print(s_df[s_df.index.duplicated(keep=False)])
                #s_df = s_df.loc[pd.to_datetime(start_date).date():pd.to_datetime(end_date).date()]
                #df[stock] = s_df[stock]
                df_arr.append(s_df)
            else:
                log(f'Download {stock} data', logtype='debug')
                symbol = stock.strip().replace('&', '_')
                symbol = symbol.replace('-', '_')
                nse_map = {'UNITDSPR': 'MCDOWELL_N',
                           'MOTHERSUMI': 'MSUMI'}
                if symbol in nse_map:
                    symbol = nse_map[symbol]
                
                s_df = cached(name=symbol, timeframe=interval)
                if s_df is None:
                    s_df = tv.get_hist(
                                symbol,
                                'NSE',
                                interval=interval,
                                n_bars=duration,
                                extended_session=False,
                            )
                    if s_df is not None:
                        cached(name=symbol, df = s_df, timeframe=interval)
                if s_df is None:
                    print(f'Error fetching information on {symbol}')
                else:
                    s_df = s_df.drop(columns = ['open', 'high', 'low', 'volume'])
                    #print(s_df.head())
                    if len(s_df)==0:
                        print('Skip {}'.format(symbol))
                        continue
                    s_df.reset_index(inplace = True)
                    s_df.rename(columns={'close': stock, 'datetime': 'date'},
                               inplace = True)
                    #print(s_df.columns)
                    #pd.to_datetime(df['DateTime']).dt.date
                    #s_df['date'] = pd.to_datetime(s_df['date'], format='%d-%m-%Y %H:%M:%S').dt.date
                    s_df['date'] = pd.to_datetime(s_df['date'], format='%d-%m-%Y %H:%M:%S')
                    if sampling=='w':
                        #Force all weekdays to start on Mondays
                        s_df['date'] = s_df['date'] - pd.to_timedelta(s_df['date'].dt.weekday, unit='D')
                        #s_df.index = s_df.index + pd.Timedelta('9 hour') +  pd.Timedelta('15 minute')
                    #s_df.drop_duplicates(inplace = True, subset='date')
                    s_df.set_index('date', inplace = True)
                    
                    s_df = s_df.sort_index()
                    s_df = s_df.reindex(columns = [stock])
                    s_df = s_df[~s_df.index.duplicated(keep='first')]
                    #print(s_df.index.values[0], type(s_df.index.values[0]))
                    #print(pd.to_datetime(start_date).date(), type(pd.to_datetime(start_date).date()))
                    #Add 1 timedelta to include the last date element as well
                    #s_df = s_df.loc[:end_date.strftime('%Y-%m-%d')+pd.Timedelta(days=1)]
                    s_df = s_df.loc[:pd.to_datetime(end_date).date()+pd.Timedelta(days=1)]
                    #print(s_df.loc[start_date:end_date])
                    #print(s_df[s_df.index.duplicated(keep=False)])
                    if len(s_df) == 0:
                        log(f'{stock} does not have data in the given range', logtype='warning')
                        continue
                    # if ((pd.to_datetime(s_df.index[0]) - df.index[0]).days > 0) and ((pd.to_datetime(s_df.index[0]) - df.index[0]).days <7):
                    #     #Handle the case of the start of the week being a holiday
                    #     data = {stock: s_df.iloc[0][stock]}
                    #     log('Handle holiday', logtype='debug')
                    #     s_df = pd.concat([s_df, pd.DataFrame(data, index=[pd.to_datetime(df.index[0])])])
                    #     #print(s_df.tail(10))
                    #     s_df.sort_index(inplace=True)
                    #     #print(s_df.head(10))
                    #     s_df.drop(s_df.index[1], inplace=True)
                    #print(s_df.head(10))
                    #df[stock] = s_df[stock]
                    #log(s_df.tail(), logtype='debug')
                    df_arr.append(s_df)
        except Stock.DoesNotExist:
            print(f'{stock} values do not exist')
    #print(df_arr)
    #Sleight of hand for now: 
    # The issue is that index df is in format DD-MM-YYYY and others are in DD-MM-YY HH-MM-SS. concat does not add them nicely.
    df = pd.concat(df_arr, axis=1)
    #s_df[sector] = df
    #print(df.tail(10))
    df = df[~df.index.duplicated(keep='first')]
    df.index.names = ['date']
    
    weights = []
    for member in list(df.columns):
        weights.append(members.get(member.strip().upper()))
    
    n_df = (df * weights)
    n_df = n_df.sum(axis=1, numeric_only=True)
    n_df = n_df.iloc[max(-duration, -len(n_df)):].to_frame()
    n_df.columns = [name]
    
    return n_df


def main(date=datetime.date.today(), sampling='w', online=True, refresh=False):
    filemapping = {}
    if refresh:
        # Load all scrips, and their attributes such as market capitalization
        data = fetch_equity_industry_data()

        # Sort the scrips as one-to-many buckets where each bucket is a theme/sector
        sectors = {}
        industries = {}
        basic_industries = {}

        for symbol, scrip in data.items():
            #print(symbol)
            if scrip.get("sector").upper().strip() not in sectors:
                sectors[scrip.get("sector").upper().strip()] = {'members':[symbol],
                                                                'member_weight':[],
                                                                'mcap': scrip['mcap']}
            else:
                sectors[scrip.get("sector").upper().strip()]['members'].append(symbol)
                sectors[scrip.get("sector").upper().strip()]['mcap'] += scrip['mcap']
            
            if scrip.get("industry").upper().strip() not in industries:
                industries[scrip.get("industry").upper().strip()] = {'members':[symbol],
                                                                    'member_weight':[],
                                                                    'mcap': scrip['mcap']}
            else:
                industries[scrip.get("industry").upper().strip()]['members'].append(symbol)
                industries[scrip.get("industry").upper().strip()]['mcap'] += scrip['mcap']

            if scrip.get("basicIndustry").upper().strip() not in basic_industries:
                basic_industries[scrip.get("basicIndustry").upper().strip()] = {'members':[symbol],
                                                                                'member_weight':[],
                                                                                'mcap': scrip['mcap']}
            else:
                basic_industries[scrip.get("basicIndustry").upper().strip()]['members'].append(symbol)
                basic_industries[scrip.get("basicIndustry").upper().strip()]['mcap'] += scrip['mcap']
        
        print("\nSectors:")
        for sector in sectors:
            # Set weights to each member of the index according to its selection criteria (market capitalization)
            for scrip in sectors[sector]['members']:
                sectors[sector]['member_weight'].append(data[scrip]['mcap']/sectors[sector]['mcap'])
            print(f"{sector}: {sectors[sector]}")
            save_custom_index(sector, sectors[sector], data, prefix='sector_')
            if len(sector)==0 or sector=='NA':
                continue
            filemapping[f'sector_{sanitize_name(sector)}'] = f'{indices_dir}/sector_{sanitize_name(sector)}.csv'

        print("\nIndustries:")
        for industry in industries:
            # Set weights to each member of the index according to its selection criteria (market capitalization)
            for scrip in industries[industry]['members']:
                industries[industry]['member_weight'].append(data[scrip]['mcap']/industries[industry]['mcap'])
            print(f"{industry}: {industries[industry]}")
            save_custom_index(industry, industries[industry], data, prefix='industry_')
            if len(industry)==0 or industry=='NA':
                continue
            filemapping[f'industry_{sanitize_name(industry)}'] = f'{indices_dir}/industry_{sanitize_name(industry)}.csv'

        print("\nBasic Industries:")
        for industry in basic_industries:
            # Set weights to each member of the index according to its selection criteria (market capitalization)
            for scrip in basic_industries[industry]['members']:
                basic_industries[industry]['member_weight'].append(data[scrip]['mcap']/basic_industries[industry]['mcap'])
            print(f"{industry}: {basic_industries[industry]}")
            save_custom_index(industry, basic_industries[industry], data, prefix='basic_industry_')
            if len(industry)==0 or industry=='NA':
                continue
            filemapping[f'basic_industry_{sanitize_name(industry)}'] = f'{indices_dir}/basic_industry_{sanitize_name(industry)}.csv'

        with open(index_map, 'w') as fd:
            fd.write(json.dumps(filemapping, indent=2))
    else:
        with open(index_map, 'r') as fd:
            filemapping = json.load(fd)
    #Load up members and compute indices for the required period
    for index, fname in filemapping.items():
        print(f'Loading {index}')
        df = get_index_dataframe(name=index, path=fname, sampling=sampling, online=online, end_date=date)
        print(df.tail(10))
        
    pass

if __name__=='__main__':
    day = datetime.date.today()
    set_loglevel('info')
    import argparse
    parser = argparse.ArgumentParser(description='Compute RRG data for indices')
    parser.add_argument('-d', '--daily', action='store_true', default = False, help="Construct indices on daily TF")
    parser.add_argument('-w', '--weekly', action='store_true', default = True, help="Construct indices on daily TF")
    parser.add_argument('-o', '--online', action='store_true', default = False, help="Fetch data from TradingView (Online)")
    parser.add_argument('-f', '--for', dest='date', help="Compute RRG for date")
    parser.add_argument('-r', '--refresh', dest='refresh', action="store_true", default=False, help="Refresh index constituents files")
    #Can add options for weekly sampling and monthly sampling later
    args = parser.parse_args()
    stock_code = None
    sampling = 'w'
    if args.daily:
        sampling='d'
        log('Daily sampling')
    if args.date is not None and len(args.date)>0:
        log(logtype='info', args = 'Get data for date: {}'.format(args.date))
        day = datetime.datetime.strptime(args.date, "%d/%m/%y")
    if args.online:
        log(logtype='info', args = 'Download data')
    else:
        log(logtype='info', args = 'Use offline data')

    pd.set_option("display.precision", 8)
    pd.options.mode.chained_assignment = None  # default='warn'
    main(date=day, sampling=sampling, online=args.online, refresh=args.refresh)