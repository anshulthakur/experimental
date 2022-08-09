'''
Created on 28-Jul-2022

@author: anshul
'''


import os
import sys
import settings
import matplotlib
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

import datetime
from django_pandas.io import read_frame
from mplfinance.original_flavor import candlestick_ohlc
import matplotlib.dates as mpl_dates

from matplotlib.dates import date2num

#Prepare to load stock data as pandas dataframe from source. In this case, prepare django
import django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'rest.settings')
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from stocks.models import Listing, Stock, Market

#Import TA-lib and backtesting library
import talib
from talib.abstract import *
from talib import MA_Type

from tvDatafeed_edge import TvDatafeed, Interval


import numpy as np
#import pandas_ta as ta
import talib as ta

#RSI Divergence strategy
#https://raposa.trade/blog/test-and-trade-rsi-divergence-in-python

from scipy.signal import argrelextrema
from collections import deque

prev_low = False

def getHigherLows(data: np.array, order=5, K=2):
  '''
  Finds consecutive higher lows in price pattern.
  Must not be exceeded within the number of periods indicated by the width 
  parameter K for the value to be confirmed.
  K determines how many consecutive lows need to be higher.
  '''
  # Get lows
  low_idx = argrelextrema(data, np.less, order=order)[0]
  lows = data[low_idx]
  # Ensure consecutive lows are higher than previous lows
  extrema = []
  ex_deque = deque(maxlen=K)
  for i, idx in enumerate(low_idx):
    if i == 0:
      ex_deque.append(idx)
      continue
    if lows[i] < lows[i-1]:
      ex_deque.clear()

    ex_deque.append(idx)
    if len(ex_deque) == K:
      extrema.append(ex_deque.copy())

  return extrema

def getLowerHighs(data: np.array, order=5, K=2):
  '''
  Finds consecutive lower highs in price pattern.
  Must not be exceeded within the number of periods indicated by the width 
  parameter for the value to be confirmed.
  K determines how many consecutive highs need to be lower.
  '''
  # Get highs
  high_idx = argrelextrema(data, np.greater, order=order)[0]
  highs = data[high_idx]
  # Ensure consecutive highs are lower than previous highs
  extrema = []
  ex_deque = deque(maxlen=K)
  for i, idx in enumerate(high_idx):
    if i == 0:
      ex_deque.append(idx)
      continue
    if highs[i] > highs[i-1]:
      ex_deque.clear()

    ex_deque.append(idx)
    if len(ex_deque) == K:
      extrema.append(ex_deque.copy())

  return extrema

def getHigherHighs(data: np.array, order=5, K=2):
  '''
  Finds consecutive higher highs in price pattern.
  Must not be exceeded within the number of periods indicated by the width 
  parameter for the value to be confirmed.
  K determines how many consecutive highs need to be higher.
  '''
  # Get highs
  high_idx = argrelextrema(data, np.greater, order=5)[0]
  highs = data[high_idx]
  # Ensure consecutive highs are higher than previous highs
  extrema = []
  ex_deque = deque(maxlen=K)
  for i, idx in enumerate(high_idx):
    if i == 0:
      ex_deque.append(idx)
      continue
    if highs[i] < highs[i-1]:
      ex_deque.clear()

    ex_deque.append(idx)
    if len(ex_deque) == K:
      extrema.append(ex_deque.copy())

  return extrema

def getLowerLows(data: np.array, order=5, K=2):
  '''
  Finds consecutive lower lows in price pattern.
  Must not be exceeded within the number of periods indicated by the width 
  parameter for the value to be confirmed.
  K determines how many consecutive lows need to be lower.
  '''
  # Get lows
  low_idx = argrelextrema(data, np.less, order=order)[0]
  lows = data[low_idx]
  # Ensure consecutive lows are lower than previous lows
  extrema = []
  ex_deque = deque(maxlen=K)
  for i, idx in enumerate(low_idx):
    if i == 0:
      ex_deque.append(idx)
      continue
    if lows[i] > lows[i-1]:
      ex_deque.clear()

    ex_deque.append(idx)
    if len(ex_deque) == K:
      extrema.append(ex_deque.copy())

  return extrema

#RSI Divergence strategy
def getHHIndex(data: np.array, order=5, K=2):
  extrema = getHigherHighs(data, order, K)
  idx = np.array([i[-1] + order for i in extrema])
  return idx[np.where(idx<len(data))]

def getLHIndex(data: np.array, order=5, K=2):
  extrema = getLowerHighs(data, order, K)
  idx = np.array([i[-1] + order for i in extrema])
  return idx[np.where(idx<len(data))]

def getLLIndex(data: np.array, order=5, K=2):
  extrema = getLowerLows(data, order, K)
  idx = np.array([i[-1] + order for i in extrema])
  return idx[np.where(idx<len(data))]

def getHLIndex(data: np.array, order=5, K=2):
  extrema = getHigherLows(data, order, K)
  idx = np.array([i[-1] + order for i in extrema])
  return idx[np.where(idx<len(data))]

def getPivots(data, key='close', order=5, K=2):
  vals = data[key].values
  hh_idx = getHHIndex(vals, order, K)
  lh_idx = getLHIndex(vals, order, K)
  ll_idx = getLLIndex(vals, order, K)
  hl_idx = getHLIndex(vals, order, K)

  data[f'{key}_highs'] = np.nan
  data[f'{key}_highs'][hh_idx] = 1
  data[f'{key}_highs'][lh_idx] = -1
  data[f'{key}_highs'] = data[f'{key}_highs'].ffill().fillna(0)
  data[f'{key}_lows'] = np.nan
  data[f'{key}_lows'][ll_idx] = 1
  data[f'{key}_lows'][hl_idx] = -1
  data[f'{key}_lows'] = data[f'{key}_highs'].ffill().fillna(0)
  return data

def getPeaks(data, key='close', order=5, K=2):
    
    vals = data[key].values
    hh_idx = getHHIndex(vals, order, K)
    lh_idx = getLHIndex(vals, order, K)
    
    a = np.empty(vals.shape)
    a[:] = np.nan
    a[hh_idx] = 1
    a[lh_idx] = -1
    #a = a.ffill().fillna(0)
    a[np.isnan(a)] = 0
    return a

def getValleys(data, key='close', order=5, K=2):
    vals = data[key].values
    ll_idx = getLLIndex(vals, order, K)
    hl_idx = getHLIndex(vals, order, K)
    
    a = np.empty(vals.shape)
    a[:] = np.nan
    a[ll_idx] = 1
    a[hl_idx] = -1
    #a = data[f'{key}_highs'].ffill().fillna(0)
    a[np.isnan(a)] = 0
    return a


def detect_divergence(exchange='NSE', symbol='NIFTY50', timeframe=Interval.in_15_minute):
    
    username = 'AnshulBot'
    password = '@nshulthakur123'
    
    tv = TvDatafeed(username, password, auto_login=True)
    
    interval = timeframe
    
    
    #n_bars = int((14.5)*60/interval)+14
    n_bars = 150
    #if exchange!='MCX':
    #    n_bars = int((6.25)*60/interval)+14
    symbol = symbol.replace('&', '_')
    symbol = symbol.replace('-', '_')
    nse_map = {'UNITDSPR': 'MCDOWELL_N',
               'MOTHERSUMI': 'MSUMI'}
    if exchange=='NSE' and symbol in nse_map:
        symbol = nse_map[symbol]
        
    df = tv.get_hist(
                symbol,
                exchange,
                interval=timeframe,
                n_bars=n_bars,
                extended_session=False,
            )
    df['RSI'] = ta.RSI(df.close, timeperiod=14)
    df.dropna(inplace=True)
    
    price = df['close'].values
    dates = df.index
    # Get higher highs, lower lows, etc.
    order = 1
    hh = getHigherHighs(price, order)
    #print(hh)
    lh = getLowerHighs(price, order)
    ll = getLowerLows(price, order)
    hl = getHigherLows(price, order)
    # Get confirmation indices
    hh_idx = np.array([min(i[1] , len(df)-1) for i in hh])
    #print(hh_idx)
    lh_idx = np.array([min(i[1] , len(df)-1) for i in lh])
    ll_idx = np.array([min(i[1] , len(df)-1) for i in ll])
    hl_idx = np.array([min(i[1] , len(df)-1) for i in hl])
    
    rsi = df['RSI'].values
    rsi_hh = getHigherHighs(rsi, order)
    rsi_lh = getLowerHighs(rsi, order)
    rsi_ll = getLowerLows(rsi, order)
    rsi_hl = getHigherLows(rsi, order)
    
    # Get confirmation indices
    rsi_hh_idx = np.array([min(i[1], len(df)-1) for i in rsi_hh])
    rsi_lh_idx = np.array([min(i[1], len(df)-1) for i in rsi_lh])
    rsi_ll_idx = np.array([min(i[1], len(df)-1) for i in rsi_ll])
    rsi_hl_idx = np.array([min(i[1], len(df)-1) for i in rsi_hl])

    close_highs = getPeaks(df, key='close', order=order)
    close_lows = getValleys(df, key='close', order=order)
    
    rsi_highs = getPeaks(df, key='RSI', order=order)
    rsi_lows = getValleys(df, key='RSI', order=order)
    
    # print(df.index[np.where(close_highs==1)])
    # print(df.index[np.where(close_highs==-1)])
    # print(df.index[np.where(close_lows==1)])
    # print(df.index[np.where(close_lows==-1)])
    #
    # print(rsi_highs)
    # print(rsi_lows)
    def get_divergence(df, index):
        #print(index)
        global prev_low
        if rsi_lows[index] == 1: #Continuation of lower low
            prev_low = True
        elif rsi_highs[index] == -1: #Continuation of lower high
            prev_low = False
            
        if close_lows[index] == 1 and (rsi_lows[index] == -1):# or prev_low==True): #Price makes lower low, indicator makes higher low (Positive divergence)
            return 1
        elif close_highs[index] == 1 and (rsi_highs[index] == -1):# or prev_low==False): #Price making higher high, RSI making lower high (Negative divergence)
            return -1
    df['divergence'] = df.apply(lambda x: get_divergence(df, df.index.get_loc(x.name)), axis=1)
    pos = df[df['divergence']==1]
    neg = df[df['divergence']==-1]
    
    
    if not pos.empty:
        #print('Positive Divergences')
        #print(pos.tail())
        pos = pos.loc[datetime.datetime.today().strftime('%Y-%m-%d 00:00:00'):]
        if not pos.empty:
            print('Positive Divergences')
            #print(pos.tail(1))
            print(pos.tail())
            #print(pos.count())
    
    if not neg.empty:
        #print('Negative Divergences')
        #print(neg.tail())
        neg = neg.loc[datetime.datetime.today().strftime('%Y-%m-%d 00:00:00'):]
        if not neg.empty:
            print('Negative Divergences')
            #print(neg.tail(1))
            print(neg.tail())
        #print(neg.count())

def convert_timeframe_to_quant(timeframe):
    if timeframe[-1].lower()=='m':
        tf = int(timeframe[0:-1])
        if tf in [1,3,5,15,30,45]:
            return eval(f'Interval.in_{tf}_minute')
        else:
            return Interval.in_15_minute
    elif timeframe[-1].lower()=='h':
        tf = int(timeframe[0:-1])
        if tf in [1,2,3,4]:
            return eval(f'Interval.in_{tf}_hour')
        else:
            return Interval.in_1_hour
    elif timeframe[-1].lower()=='d':
        return Interval.in_daily
    elif timeframe[-1].lower()=='w':
        return Interval.in_weekly
    elif timeframe[-1].lower()=='m':
        return Interval.in_monthly
    else:
        print(f'Unknown timeframe {timeframe}')
        return Interval.in_15_minute

def main(stock_name=None, exchange = 'NSE', timeframe= '15m', date=None, category = ''):
    #List of FnO stocks (where bullish and bearish signals are required)
    timeframe=convert_timeframe_to_quant(timeframe)
    fno = []
    with open('fno.txt', 'r') as fd:
        for line in fd:
            fno.append(line.strip())
    fno = sorted(fno)
    
    portfolio = []
    margin_stocks = []
    if category != 'fno':
        with open('portfolio.txt', 'r') as fd:
            for line in fd:
                portfolio.append(line.strip())
        portfolio = sorted(portfolio)
        
        with open('margin.txt', 'r') as fd:
            for line in fd:
                margin_stocks.append(line.strip())
            
    if stock_name is None:
        for sname in fno:
            try:
                print(sname)
                detect_divergence(exchange=exchange, symbol=sname, timeframe=timeframe)
            except:
                print(f'Error in {sname}')
        if category != 'fno':
            for sname in portfolio:
                try:
                    print(sname)
                    detect_divergence(exchange=exchange, symbol=sname, timeframe=timeframe)
                except:
                    print(f'Error in {sname}')
            for stock in Stock.objects.all():
                #listing = get_stock_listing(stock, duration=30, last_date = datetime.date(2020, 12, 31))
                #print(stock)
                if (stock.sid in fno) or (stock.sid in portfolio):
                    continue
                if stock.sid in margin_stocks:
                    print(sname)
                    detect_divergence(exchange=exchange, symbol=stock.sname, timeframe=timeframe)
                else:
                    print(sname)
                    detect_divergence(exchange=exchange, symbol=stock.sname, timeframe=timeframe)
    else:
        print(f'{exchange}:{stock_name} over TF: {timeframe}')
        detect_divergence(exchange=exchange, symbol=stock_name, timeframe=timeframe)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Scan stock info')
    parser.add_argument('-s', '--stock', help="Stock code")
    parser.add_argument('-e', '--exchange', help="Exchange")
    parser.add_argument('-d', '--date', help="Date")
    parser.add_argument('-t', '--timeframe', help="Timeframe")
    parser.add_argument('-f', '--fno', help="Scan FnO stocks only", action='store_true', default=False)
    parser.add_argument('-c', '--category', help="Category:One of 'all', 'fno', 'margin', 'portfolio'")
    args = parser.parse_args()
    stock_code = None
    day = None
    timeframe = '15m'
    
    category = 'all'
    exchange = 'NSE'
    if args.stock is not None and len(args.stock)>0:
        print('Scan data for stock {}'.format(args.stock))
        stock_code = args.stock
    if args.exchange is not None and len(args.exchange)>0:
        exchange=args.exchange
    if args.timeframe is not None and len(args.timeframe)>0:
        timeframe=args.timeframe
    if args.date is not None and len(args.date)>0:
        print('Scan data for date: {}'.format(args.date))
        day = datetime.datetime.strptime(args.date, "%d/%m/%y")
    if args.fno is True:
        category = 'fno'
    if args.category is not None:
        category = args.category

    print('Scan for {}'.format(category))
    main(stock_code, exchange, timeframe = timeframe, category=category, date=day)
