'''
Created on 12-Apr-2022

@author: anshul
'''
import os
import sys
import settings
import matplotlib
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

from django_pandas.io import read_frame
from mplfinance.original_flavor import candlestick_ohlc
import matplotlib.dates as mpl_dates

from matplotlib.dates import date2num

# imports
import pandas_datareader.data as pdr
import datetime
import talib
from talib.abstract import *
from talib import MA_Type

# format price data
pd.options.display.float_format = '{:0.2f}'.format

#Prepare to load stock data as pandas dataframe from source. In this case, prepare django
import django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'rest.settings')
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from stocks.models import Listing, Stock


import datetime
from pandas.tseries.frequencies import to_offset

def get_stock_listing(stock, duration=None, last_date = datetime.date.today(), studies=None, resample=False, monthly=False):
    requested_duration = duration
    if duration == None and studies==None:
        duration = 365
        requested_duration = duration
    elif duration == -1:
        duration = 3650
    elif duration is not None and resample==True:
        if monthly is False:
            duration = duration*5
        else:
            duration = duration*30
    elif studies is not None:
        #studies={'daily': ['rsi', 'ema20', 'ema10', 'sma200'],
        #                   'weekly':['rsi', 'ema20', 'ema10'],
        #                   'monthly': ['rsi']}
        if studies.get('monthly', None) is not None:
            if 'rsi' in studies.get('monthly'):
                duration = 500 #Need at least 14 months data for RSI
        elif studies.get('weekly', None) is not None:
            if 'rsi' in studies.get('weekly'):
                duration = 14*7 if duration is None else max(duration, 14*7)#Need at least 14 weeks data for RSI
            if 'ema20' in studies.get('weekly'):
                duration = 21*7 if duration is None else max(duration, 14*7)#Need at least 20 weeks data for EMA
            if 'ema10' in studies.get('weekly'):
                duration = 11*7 if duration is None else max(duration, 11*7)#Need at least 20 weeks data for EMA
        elif studies.get('daily', None) is not None:
            if 'rsi' in studies.get('daily'):
                duration = 14 if duration is None else max(duration, 14)#Need at least 14 days data for RSI
            if 'ema20' in studies.get('daily'):
                duration = 21 if duration is None else max(duration, 21)#Need at least 20 days data for EMA
            if 'ema10' in studies.get('daily'):
                duration = 11 if duration is None else max(duration, 11)#Need at least 10 days data for EMA
            if 'sma200' in studies.get('daily'):
                duration = 291 if duration is None else max(duration, 291)#Need at least 291 days data for SMA
                
    #print(duration)
    first_date = last_date - datetime.timedelta(days=duration)
    #print(first_date)
    listing = Listing.objects.filter(stock=stock, date__range=(first_date, last_date))
    df = read_frame(listing, index_col='date')
    for column in df.columns:
        if column != 'stock':
           df[column] = pd.to_numeric(df[column])
    df = df.sort_index()
    df = df.reindex(columns = ['open', 'high', 'low', 'close', 'traded', 'deliverable', 'trades'])
    df.rename(columns={"traded":"volume", 
                       'deliverable':'delivery'}, inplace=True)
    df.index = pd.to_datetime(df.index)

    #Delete duplicate columns
    #df = df.loc[:,~df.columns.duplicated()]
    df.drop_duplicates(inplace = True)
    #df.dropna(inplace=True)
    if resample:
        #Resample weekly
        logic = {'open'  : 'first',
                 'high'  : 'max',
                 'low'   : 'min',
                 'close' : 'last',
                 'volume': 'sum',
                 'delivery': 'sum',
                 'trades': 'sum'}
        #Resample on weekly levels
        if monthly:
            df = df.resample('M').apply(logic)
        else:
            df = df.resample('W').apply(logic)
            df.index -= to_offset("6D")
        
    #Add the studies inplace 
    if studies is not None and studies.get('daily', None) is not None and len(df)>0:
        if 'rsi' in studies.get('daily'):
            df['rsi'] = talib.RSI(df['close'], 14)
        if 'ema20' in studies.get('daily'):
            df['ema20'] = talib.EMA(df['close'], 20)
        if 'ema10' in studies.get('daily'):
            df['ema10'] = talib.EMA(df['close'], 10)
        if 'sma200' in studies.get('daily'):
            df['sma200'] = talib.SMA(df['close'], 200)
    if studies is not None and studies.get('weekly', None) is not None and len(df)>0:
        if 'rsi' in studies.get('weekly'):
            df['rsi'] = talib.RSI(df['close'], 14)
        if 'ema20' in studies.get('daily'):
            df['ema20'] = talib.EMA(df['close'], 20)
        if 'ema10' in studies.get('daily'):
            df['ema10'] = talib.EMA(df['close'], 10)
    return df
