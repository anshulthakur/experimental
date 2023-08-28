
import os
import sys
import csv
import pandas as pd
import numpy as np
from pandas.tseries.frequencies import to_offset
import datetime

from stocks.models import Stock, Market
from lib.tradingview import TvDatafeed, Interval, convert_timeframe_to_quant, get_tvfeed_instance
from lib.retrieval import get_stock_listing
import init
from init import project_dirs
from lib.cache import cached
from lib.logging import log

import pytz

index_data_dir = project_dirs['reports']
member_dir = index_data_dir+'/members/'
plotpath = index_data_dir+'/plots/'


INDICES = {"NIFTY 50":"Nifty_50",
           "NIFTY AUTO":"Nifty_Auto",
           "NIFTY BANK":"Nifty_Bank",
           "NIFTY ENERGY": "Nifty_Energy",
           "NIFTY FINANCIAL SERVICES": "Nifty_Financial_Services",
           "NIFTY FMCG": "Nifty_FMCG",
           "NIFTY IT": "Nifty_IT",
           "NIFTY MEDIA": "Nifty_Media",
           "NIFTY METAL": "Nifty_Metal",
           "NIFTY MNC": "Nifty_MNC",
           "NIFTY PHARMA": "Nifty_Pharma",
           "NIFTY PSU BANK": "Nifty_PSU_Bank",
           "NIFTY REALTY": "Nifty_Realty",
           "NIFTY INDIA CONSUMPTION": "Nifty_India_Consumption",
           "NIFTY COMMODITIES": "Nifty_Commodities",
           "NIFTY INFRASTRUCTURE": "Nifty_Infrastructure",
           "NIFTY PSE": "Nifty_PSE",
           "NIFTY SERVICES SECTOR": "Nifty_Services_Sector",
           "NIFTY GROWTH SECTORS 15": "Nifty_Growth_Sectors_15",
           "NIFTY SME EMERGE": "NIFTY_SME_EMERGE",
           "NIFTY OIL & GAS": "Nifty_Oil_&_Gas",
           "NIFTY HEALTHCARE INDEX": "Nifty_Healthcare_Index",
           "NIFTY TOTAL MARKET": "Nifty_Total_Market",
           "NIFTY INDIA DIGITAL": "Nifty_India_Digital",
           "NIFTY MOBILITY": "Nifty_Mobility",
           "NIFTY INDIA DEFENCE": "Nifty_India_Defence",
           "NIFTY FINANCIAL SERVICES EX BANK": "Nifty_Financial_Services_Ex_Bank",
           "NIFTY HOUSING": "Nifty_Housing",
           "NIFTY TRANSPORTATION & LOGISTICS": "Nifty_Transportation_&_Logistics",
           "NIFTY MIDSMALL FINANCIAL SERVICES": "Nifty_MidSmall_Financial_Services",
           "NIFTY MIDSMALL HEALTHCARE": "Nifty_MidSmall_Healthcare",
           "NIFTY MIDSMALL IT & TELECOM": "Nifty_MidSmall_IT_&_Telecom",
           "NIFTY CONSUMER DURABLES": "Nifty_Consumer_Durables",
           "NIFTY NON CYCLICAL CONSUMER": "Nifty_Non_Cyclical_Consumer",
           "NIFTY INDIA MANUFACTURING": "Nifty_India_Manufacturing",
           "NIFTY NEXT 50": "Nifty_Next_50",
           "NIFTY 100": "Nifty_100",
           "NIFTY 200": "Nifty_200",
           "NIFTY 500": "Nifty_500",
           "NIFTY MIDCAP 50": "Nifty_Midcap_50",
           "NIFTY MIDCAP 100": "NIFTY_Midcap_100",
           "NIFTY SMALLCAP 100": "NIFTY_Smallcap_100",
           "NIFTY100 EQUAL WEIGHT": "Nifty100_Equal_Weight",
           "NIFTY100 LIQUID 15": "Nifty100_Liquid_15",
           "NIFTY CPSE": "Nifty_CPSE",
           "NIFTY50 VALUE 20": "Nifty50_Value_20",
           "NIFTY MIDCAP LIQUID 15": "Nifty_Midcap_Liquid_15",
           "NIFTY100 QUALITY 30": "NIFTY100_Quality_30",
           "NIFTY PRIVATE BANK": "Nifty_Private_Bank",
           "NIFTY SMALLCAP 250": "Nifty_Smallcap_250",
           "NIFTY SMALLCAP 50": "Nifty_Smallcap_50",
           "NIFTY MIDSMALLCAP 400": "Nifty_MidSmallcap_400",
           "NIFTY MIDCAP 150": "Nifty_Midcap_150",
           "NIFTY MIDCAP SELECT": "Nifty_Midcap_Select",
           "NIFTY LARGEMIDCAP 250": "NIFTY_LargeMidcap_250",
           "NIFTY FINANCIAL SERVICES 25 50": "Nifty_Financial_Services_25_50",
           "NIFTY500 MULTICAP 50 25 25": "Nifty500_Multicap_50_25_25",
           "NIFTY MICROCAP 250": "Nifty_Microcap_250",
           "NIFTY200 MOMENTUM 30": "Nifty200_Momentum_30",
           "NIFTY100 ALPHA 30": "NIFTY100_Alpha_30",
           "NIFTY500 VALUE 50": "NIFTY500_Value_50",
           "NIFTY100 LOW VOLATILITY 30": "Nifty100_Low_Volatility_30",
           "NIFTY ALPHA LOW VOLATILITY 30": "NIFTY_Alpha_Low_Volatility_30",
           "NIFTY QUALITY LOW VOLATILITY 30": "NIFTY_Quality_Low_Volatility_30",
           "NIFTY ALPHA QUALITY LOW VOLATILITY 30": "NIFTY_Alpha_Quality_Low_Volatility_30",
           "NIFTY ALPHA QUALITY VALUE LOW VOLATILITY 30": "NIFTY_Alpha_Quality_Value_Low_Volatility_30",
           "NIFTY200 QUALITY 30": "NIFTY200_Quality_30",
           "NIFTY MIDCAP150 QUALITY 50": "NIFTY_Midcap150_Quality_50",
           "NIFTY200 ALPHA 30": "Nifty200_Alpha_30",
           "NIFTY MIDCAP150 MOMENTUM 50": "Nifty_Midcap150_Momentum_50",
           "NIFTY50 EQUAL WEIGHT": "NIFTY50_Equal_Weight"
        }

def load_index_members(sector, members, date=datetime.datetime.now(), interval=Interval.in_weekly, 
                        entries=50, online=True, start_date=None, end_date=None, market='NSE'):
    
    def resample(df, interval):
        df_offset_str = '09h15min'
        logic = {'open'  : 'first',
                 'high'  : 'max',
                 'low'   : 'min',
                 'close' : 'last',
                 'volume': 'sum',
                 'delivery': 'sum',
                 'trades': 'sum'}
        int_val = None
        try:
            int_val = int(interval)
            int_val = f'{interval}min'
        except:
            int_val = interval

        df = df.resample(interval.value, offset=df_offset_str).apply(logic).dropna()
        return df
    
    log('========================', 'debug')
    log(f'Loading for {sector}', 'debug')
    log('========================', 'debug')

    username = 'AnshulBot'
    password = '@nshulthakur123'
    tv = None
    if not online:
        if interval.to_days()<1.0:
            online = True
    if online:
        tv = get_tvfeed_instance(username, password)
        #print(duration, type(duration))
    df = None 

    pacific = pytz.timezone('US/Pacific')
    india = pytz.timezone('Asia/Calcutta')
    s_list = []
    skipped = []

    duration = entries * interval.to_days()
    for stock in members:
        try:
            if not online:
                if ':' in stock:
                    market = stock.split(':')[0]
                    stock = stock.split(':')[1]
                stock_obj = Stock.objects.get(symbol=stock, market=Market.objects.get(name=market))
                s_df = get_stock_listing(stock_obj, duration=duration, last_date = date)
                s_df = resample(s_df, interval)
                s_df = s_df.drop(columns = ['open', 'high', 'low', 'volume', 'delivery', 'trades'])
                #print(s_df.head())
                if len(s_df)==0:
                    skipped.append(stock_obj.symbol)
                    continue
                s_df.reset_index(inplace = True)
                s_df.rename(columns={'close': stock,
                                     'date': 'datetime'},
                            inplace = True)
                s_df['datetime'] = pd.to_datetime(s_df['datetime'], format='%d-%m-%Y %H:%M:%S')
                s_df.set_index('datetime', inplace = True)
                s_df = s_df.sort_index()
                s_df = s_df.reindex(columns = [stock])
                s_df = s_df[~s_df.index.duplicated(keep='first')]
                if start_date is not None and end_date is not None:
                    s_df = s_df.loc[pd.to_datetime(start_date).date():pd.to_datetime(end_date).date()]
                s_list.append(s_df)
            else:
                symbol = stock.strip().replace('&', '_')
                symbol = symbol.replace('-', '_')
                nse_map = {'UNITDSPR': 'MCDOWELL_N',
                           'MOTHERSUMI': 'MSUMI'}
                if symbol in nse_map:
                    symbol = nse_map[symbol]
                
                s_df = cached(name=symbol, timeframe=interval)
                if s_df is not None:
                    pass
                else:
                    s_df = tv.get_hist(
                                symbol,
                                market,
                                interval=interval,
                                n_bars=entries,
                                extended_session=False,
                            )
                    if s_df is not None:
                        cached(name=symbol, df=s_df, timeframe=interval)
                if s_df is None:
                    log(f'Error fetching information on {symbol}', 'warning')
                else:
                    s_df = s_df.drop(columns = ['open', 'high', 'low', 'volume'])
                    #print(s_df.head())
                    if len(s_df)==0:
                        log('Skip {}'.format(symbol), 'info')
                        continue
                    s_df.reset_index(inplace = True)
                    s_df.rename(columns={'close': stock},
                               inplace = True)
                    #print(s_df.columns)
                    #pd.to_datetime(df['DateTime']).dt.date
                    s_df['datetime'] = pd.to_datetime(s_df['datetime'], format='%d-%m-%Y %H:%M:%S')
                    #s_df.drop_duplicates(inplace = True, subset='date')
                    s_df.set_index('datetime', inplace = True)
                    s_df.index = s_df.index.tz_localize(pacific).tz_convert(india).tz_convert(None)
                    s_df = s_df.sort_index()
                    s_df = s_df.reindex(columns = [stock])
                    s_df = s_df[~s_df.index.duplicated(keep='first')]
                    #print(s_df.index.values[0], type(s_df.index.values[0]))
                    if start_date is not None and end_date is not None:
                        #print(pd.to_datetime(start_date).date(), type(pd.to_datetime(start_date).date()))
                        s_df = s_df.loc[pd.to_datetime(start_date).date():pd.to_datetime(end_date).date()]
                    #print(s_df.loc[start_date:end_date])
                    #print(s_df.head(10))
                    #print(s_df[s_df.index.duplicated(keep=False)])
                    s_list.append(s_df)
                    #df[stock] = s_df[stock]
        except Stock.DoesNotExist:
            log(f'{stock} values do not exist', 'error')
        except Market.DoesNotExist:
            log(f'{market} does not exist as Market', 'error')
    df = pd.concat(s_list, axis='columns')
    df = df[~df.index.duplicated(keep='first')]
    df.sort_index(inplace=True)
    #print(df.head(10))
    log(f'Skiped {skipped}', 'info')
    return df

def load_members(sector, members, date, sampling=Interval.in_weekly, entries=50, online=True):
    print('========================')
    print(f'Loading for {sector}')
    print('========================')
    
    df = pd.read_csv(f'{index_data_dir}{INDICES[sector]}.csv')
    df.rename(columns={'Index Date': 'date',
                    'Closing Index Value': INDICES[sector]},
            inplace = True)
    df['date'] = pd.to_datetime(df['date'], format='%d-%m-%Y')
    df.set_index('date', inplace = True)
    df = df.sort_index()
    df = df.reindex(columns = [INDICES[sector]])
    df = df[~df.index.duplicated(keep='first')]
    
    if date is not None:
        df = df[:date.strftime('%Y-%m-%d')]
    if sampling.value =='1W':
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
    if sampling.value=='1W':
        duration = duration.astype('timedelta64[W]')/np.timedelta64(1, 'W')
    else:
        duration = duration.astype('timedelta64[D]')/np.timedelta64(1, 'D')
    
    duration = max(int(duration.astype(int))+1, entries)

    username = 'AnshulBot'
    password = '@nshulthakur123'
    tv = None
    interval = sampling
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

def get_index_members(name):
    members = []
    if name not in INDICES:#MEMBER_MAP:
        log(f'{name} not in list', 'error')
        return members
    #with open('./indices/members/'+MEMBER_MAP[name], 'r', newline='') as csvfile:
    with open(f'{member_dir}{INDICES[name]}.csv', 'r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            members.append(row['Symbol'].strip())
    return members
