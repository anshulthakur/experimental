
import os
import sys
import ..settings
import csv
import pandas as pd
import numpy as np
from pandas.tseries.frequencies import to_offset
import datetime

import django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'rest.settings')
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from stocks.models import Listing, Stock
from lib.tradingview import TvDatafeed, Interval, convert_timeframe_to_quant, get_tvfeed_instance
from lib.retrieval import get_stock_listing

index_data_dir = '../reports/'
member_dir = '../reports/members/'
plotpath = index_data_dir+'plots/'
cache_dir = index_data_dir+'cache/'

INDICES = ["Nifty_50",
           "Nifty_Auto",
           "Nifty_Bank",
           "Nifty_Energy",
           "Nifty_Financial_Services",
           "Nifty_FMCG",
           "Nifty_IT",
           "Nifty_Media",
           "Nifty_Metal",
           "Nifty_MNC",
           "Nifty_Pharma",
           "Nifty_PSU_Bank",
           "Nifty_Realty",
           "Nifty_India_Consumption",
           "Nifty_Commodities",
           "Nifty_Infrastructure",
           "Nifty_PSE",
           "Nifty_Services_Sector",
           "Nifty_Growth_Sectors_15",
           "NIFTY_SME_EMERGE",
           "Nifty_Oil_&_Gas",
           "Nifty_Healthcare_Index",
           "Nifty_Total_Market",
           "Nifty_India_Digital",
           "Nifty_Mobility",
           "Nifty_India_Defence",
           "Nifty_Financial_Services_Ex_Bank",
           "Nifty_Housing",
           "Nifty_Transportation_&_Logistics",
           "Nifty_MidSmall_Financial_Services",
           "Nifty_MidSmall_Healthcare",
           "Nifty_MidSmall_IT_&_Telecom",
           "Nifty_Consumer_Durables",
           "Nifty_Non_Cyclical_Consumer",
           "Nifty_India_Manufacturing",
           "Nifty_Next_50",
           "Nifty_100",
           "Nifty_200",
           "Nifty_500",
           "Nifty_Midcap_50",
           "NIFTY_Midcap_100",
           "NIFTY_Smallcap_100",
           #"Nifty_Dividend_Opportunities_50",
           #"Nifty_Low_Volatility_50",
           #"Nifty_Alpha_50",
           #"Nifty_High_Beta_50",
           "Nifty100_Equal_Weight",
           "Nifty100_Liquid_15",
           "Nifty_CPSE",
           "Nifty50_Value_20",
           "Nifty_Midcap_Liquid_15",
           "NIFTY100_Quality_30",
           "Nifty_Private_Bank",
           "Nifty_Smallcap_250",
           "Nifty_Smallcap_50",
           "Nifty_MidSmallcap_400",
           "Nifty_Midcap_150",
           "Nifty_Midcap_Select",
           "NIFTY_LargeMidcap_250",
           "Nifty_Financial_Services_25_50",
           "Nifty500_Multicap_50_25_25",
           "Nifty_Microcap_250",
           "Nifty200_Momentum_30",
           "NIFTY100_Alpha_30",
           "NIFTY500_Value_50",
           "Nifty100_Low_Volatility_30",
           "NIFTY_Alpha_Low_Volatility_30",
           "NIFTY_Quality_Low_Volatility_30",
           "NIFTY_Alpha_Quality_Low_Volatility_30",
           "NIFTY_Alpha_Quality_Value_Low_Volatility_30",
           "NIFTY200_Quality_30",
           "NIFTY_Midcap150_Quality_50",
           "Nifty200_Alpha_30",
           "Nifty_Midcap150_Momentum_50",
           "NIFTY50_Equal_Weight",
           "Nifty_Total_Market",
           ]


def cached(name, df=None):
    import json
    cache_file = '.cache.json'
    overwrite = False
    try:
        with open(cache_dir+cache_file, 'r') as fd:
            progress = json.load(fd)
            try:
                date = datetime.datetime.strptime(progress['date'], '%d-%m-%Y')
                if date.day == datetime.datetime.today().day and \
                    date.month == datetime.datetime.today().month and \
                    date.year == datetime.datetime.today().year:
                    pass #Cache hit
                else:
                    if df is None:#Cache is outdated. Clear it first
                        for f in os.listdir(cache_dir):
                            if f != cache_dir+cache_file:
                                os.remove(os.path.join(cache_dir, f))
                    overwrite = True
            except:
                #Doesn't look like a proper date time
                pass
    except:
        overwrite=True
    
    if overwrite:
        with open(cache_dir+cache_file, 'w') as fd:
            fd.write(json.dumps({'date':datetime.datetime.today().strftime('%d-%m-%Y')}))
    
    f = cache_dir+name+'.csv'
    if df is None:
        if os.path.isfile(f):
            #Get from cache if it exists
            df = pd.read_csv(f)
            df['datetime'] = pd.to_datetime(df['datetime'], format='%Y-%m-%d %H:%M:%S')
            return df
        return None
    else:
        #Cache the results
        df.to_csv(f)
        return None

def load_members(sector, members, date, sampling='w', entries=50, online=True):
    print('========================')
    print(f'Loading for {sector}')
    print('========================')
    
    df = pd.read_csv(f'{index_data_dir}{sector}.csv')
    df.rename(columns={'Index Date': 'date',
                       'Closing Index Value': sector},
               inplace = True)
    df['date'] = pd.to_datetime(df['date'], format='%d-%m-%Y')
    df.set_index('date', inplace = True)
    df = df.sort_index()
    df = df.reindex(columns = [sector])
    df = df[~df.index.duplicated(keep='first')]
    
    if date is not None:
        df = df[:date.strftime('%Y-%m-%d')]
    if sampling=='w':
        #Resample weekly
        logic = {}
        for cols in df.columns:
            if cols != 'date':
                logic[cols] = 'last'
        #Resample on weekly levels
        df = df.resample('W').apply(logic)
        #df = df.resample('W-FRI', closed='left').apply(logic)
        df.index -= to_offset("6D")
    #Truncate to last n days
    df = df.iloc[-entries:]
    #print(df.head(10))
    #print(date)
    start_date = df.index.values[0]
    end_date = df.index.values[-1]
    #print(start_date, type(start_date))

    #print(np.datetime64(date))
    duration = np.datetime64(datetime.datetime.today())-start_date
    if sampling=='w':
        duration = duration.astype('timedelta64[W]')/np.timedelta64(1, 'W')
    else:
        duration = duration.astype('timedelta64[D]')/np.timedelta64(1, 'D')
    
    duration = max(int(duration.astype(int))+1, entries)

    username = 'AnshulBot'
    password = '@nshulthakur123'
    tv = None
    interval = convert_timeframe_to_quant(sampling)
    if online:
        tv = get_tvfeed_instance(username, password)
    #print(duration, type(duration))
    for stock in members:
        try:
            if not online:
                stock_obj = Stock.objects.get(sid=stock)
                s_df = get_stock_listing(stock_obj, duration=duration, last_date = date)
                s_df = s_df.drop(columns = ['open', 'high', 'low', 'volume', 'delivery', 'trades'])
                #print(s_df.head())
                if len(s_df)==0:
                    print('Skip {}'.format(stock_obj))
                    continue
                s_df.rename(columns={'close': stock},
                           inplace = True)
                s_df.reset_index(inplace = True)
                s_df['date'] = pd.to_datetime(s_df['date'], format='%d-%m-%Y')
                #s_df.drop_duplicates(inplace = True, subset='date')
                s_df.set_index('date', inplace = True)
                s_df = s_df.sort_index()
                s_df = s_df.reindex(columns = [stock])
                s_df = s_df[~s_df.index.duplicated(keep='first')]
                #print(s_df[s_df.index.duplicated(keep=False)])
                s_df = s_df.loc[pd.to_datetime(start_date).date():pd.to_datetime(end_date).date()]
                df[stock] = s_df[stock]
            else:
                print(stock)
                symbol = stock.strip().replace('&', '_')
                symbol = symbol.replace('-', '_')
                nse_map = {'UNITDSPR': 'MCDOWELL_N',
                           'MOTHERSUMI': 'MSUMI'}
                if symbol in nse_map:
                    symbol = nse_map[symbol]
                
                s_df = cached(symbol)
                if s_df is not None:
                    pass
                else:
                    s_df = tv.get_hist(
                                symbol,
                                'NSE',
                                interval=interval,
                                n_bars=duration,
                                extended_session=False,
                            )
                    if s_df is not None:
                        cached(symbol, s_df)
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
                    s_df['date'] = pd.to_datetime(s_df['date'], format='%d-%m-%Y').dt.date
                    #s_df.drop_duplicates(inplace = True, subset='date')
                    s_df.set_index('date', inplace = True)
                    s_df = s_df.sort_index()
                    s_df = s_df.reindex(columns = [stock])
                    s_df = s_df[~s_df.index.duplicated(keep='first')]
                    #print(s_df.index.values[0], type(s_df.index.values[0]))
                    #print(pd.to_datetime(start_date).date(), type(pd.to_datetime(start_date).date()))
                    s_df = s_df.loc[pd.to_datetime(start_date).date():pd.to_datetime(end_date).date()]
                    #print(s_df.loc[start_date:end_date])
                    #print(s_df.head(10))
                    #print(s_df[s_df.index.duplicated(keep=False)])
                    df[stock] = s_df[stock]
        except Stock.DoesNotExist:
            print(f'{stock} values do not exist')
    df = df[~df.index.duplicated(keep='first')]
    
    #print(df.head(10))
    return df

def load_index_members(name):
    members = []
    if name not in INDICES:#MEMBER_MAP:
        print(f'{name} not in list')
        return members
    #with open('./indices/members/'+MEMBER_MAP[name], 'r', newline='') as csvfile:
    with open(f'{member_dir}{name}.csv', 'r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            members.append(row['Symbol'].strip())
    return members
