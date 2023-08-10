import os
import sys
import init
import matplotlib
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

from django_pandas.io import read_frame
from mplfinance.original_flavor import candlestick_ohlc
import matplotlib.dates as mpl_dates

from matplotlib.dates import date2num

import talib
from talib.abstract import *
from talib import MA_Type

from backtesting import Strategy
from backtesting.lib import crossover
from backtesting import Backtest

from lib.pivots import getHHIndex, getHLIndex, getLLIndex, getLHIndex

from tvDatafeed_edge import TvDatafeed, Interval

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

def print_debug(args):
    debug = True
    if debug: 
        print(args)
    return

#Create a strategy
class TrendFollow(Strategy):
    # Define the two MA lags as *class variables*
    # for later optimization
    order = 1
    
    sl = 0
    
    start_hr = 9
    start_min = 0
    
    start_offset = 5
    
    end_hr = 23
    end_min = 15 
    
    tolerance = 2
    
    def init(self):
        # Precompute the two moving averages
        pass
    
    def next(self):
        hh_val = 0
        hl_val = 0
        lh_val = 0
        ll_val = 0
        d = self.data.df.index[-1].to_pydatetime()
        if (d.hour == self.start_hr and d.minute==self.start_min) or (d.hour == self.end_hr and d.minute>self.end_min):
            hh_val = 0
            hl_val = 0
            lh_val = 0
            ll_val = 0
            self.sl = 0
            return
        elif d.hour == self.start_hr and d.minute< (self.start_min + self.start_offset):
            #No decision making in the first 5 minutes
            return
        elif d.hour==self.end_hr and d.minute==self.end_min:
            if self.position:
                print_debug('Close position')
                self.position.close()
                return
        hh = getHHIndex(self.data.High, order=self.order)
        if hh is not None and len(hh)>1:
            hh = hh[-1]
            if abs(hh_val - self.data.High[hh])>self.tolerance:
                hh_val = self.data.High[hh]
        else:
            hh = -1
        hl = getHLIndex(self.data.Low, order=self.order)
        if hl is not None and len(hl)>1:
            if self.data.Low[hl[0]] < ll_val:
                #When trend changes, the last low won't be marked (
                ll = hl[0]
                ll_val = self.data.Low[ll]
            hl = hl[-1]
            if abs(hl_val - self.data.Low[hl])>self.tolerance:
                hl_val = self.data.Low[hl]
        else:
            hl = -1
        ll = getLLIndex(self.data.Low, order=self.order)
        if ll is not None and len(ll)>1:
            ll = ll[-1]
            if abs(ll_val - self.data.Low[ll])>self.tolerance:
                ll_val = self.data.Low[ll]
        else:
            ll = -1
        lh = getLHIndex(self.data.High, order=self.order)
        if lh is not None and len(lh)>1:
            if self.data.High[lh[0]] > hh_val:
                hh = lh[0]
                hh_val = self.data.High[hh]
            lh = lh[-1]
            if abs(lh_val - self.data.High[lh])>self.tolerance:
                lh_val = self.data.High[lh]
        else:
            lh = -1
        
        print_debug(f'D: {self.data.df.index[-1]} HH:{hh}({hh_val}) HL:{hl}({hl_val}) LH:{lh}({lh_val}) LL:{ll}({ll_val}) ')
        if hh>ll and hl>ll:
            #Colud be uptrend
            if hh > hl:
                #uptrend confirm
                if not self.position  and self.data.Close[-1] > hl_val:
                    print_debug('Long')
                    #print_debug(self.data.df.tail(1))
                    self.sl = hl_val
                    print_debug(f'D: {self.data.df.index[-1]} O:{self.data.Open[-1]} H:{self.data.High[-1]} L:{self.data.Low[-1]} C:{self.data.Close[-1]} SL:{self.sl}')
                    self.buy(sl = self.sl)
                elif self.position.is_short  and self.data.Close[-1] > hl_val:
                    print_debug('Long')
                    #print_debug(self.data.df.tail(1))
                    self.sl = hl_val
                    print_debug(f'D: {self.data.df.index[-1]} O:{self.data.Open[-1]} H:{self.data.High[-1]} L:{self.data.Low[-1]} C:{self.data.Close[-1]} SL:{self.sl}')
                    self.buy(sl = self.sl)
                else:
                    if self.sl != hl_val:
                        self.sl = hl_val
                        print_debug(f'D: {self.data.df.index[-1]} SL: {self.sl}')
                    #Else, update stop loss
            else:
                if hl > hh and self.data.Close[-1] > hh_val:
                    #uptrend pending confirm, but candle just closed above previous high
                    if not self.position and self.data.Close[-1] > hl_val:
                        print_debug('Long')
                        #print_debug(self.data.df.tail(1))
                        self.sl = hl_val
                        print_debug(f'D: {self.data.df.index[-1]} O:{self.data.Open[-1]} H:{self.data.High[-1]} L:{self.data.Low[-1]} C:{self.data.Close[-1]} SL:{self.sl}')
                        self.buy(sl = self.sl)
                    elif self.position.is_short and self.data.Close[-1] > hl_val:
                        print_debug('Long')
                        #print_debug(self.data.df.tail(1))
                        self.sl = hl_val
                        print_debug(f'D: {self.data.df.index[-1]} O:{self.data.Open[-1]} H:{self.data.High[-1]} L:{self.data.Low[-1]} C:{self.data.Close[-1]} SL:{self.sl}')
                        self.buy(sl = self.sl)
                    else:
                        if self.sl != hl_val:
                            self.sl = hl_val
                            print_debug(f'D: {self.data.df.index[-1]} SL: {self.sl}')
                        #Else, update stop loss
        if lh > hh and lh > hl:
            #downtrend possible
            if ll > lh:
                #downtrend confirm
                if not self.position and self.data.Close[-1] < lh_val:
                    print_debug('Short')
                    #print_debug(self.data.df.tail(1))
                    self.sl = lh_val
                    print_debug(f'D: {self.data.df.index[-1]} O:{self.data.Open[-1]} H:{self.data.High[-1]} L:{self.data.Low[-1]} C:{self.data.Close[-1]} SL:{self.sl}')
                    self.sell(sl=self.sl)
                elif self.position.is_long  and self.data.Close[-1] < lh_val:
                    print_debug('Short')
                    #print_debug(self.data.df.tail(1))
                    self.sl = lh_val
                    print_debug(f'D: {self.data.df.index[-1]} O:{self.data.Open[-1]} H:{self.data.High[-1]} L:{self.data.Low[-1]} C:{self.data.Close[-1]} SL:{self.sl}')
                    self.sell(sl=self.sl)
                else:
                    #Else, update stoploss
                    if self.position and self.sl != lh_val:
                        self.sl = lh_val
                        print_debug(f'D: {self.data.df.index[-1]} SL: {self.sl}')
            else:
                if lh > ll and self.data.Close[-1] < ll_val:
                    #downtrend confirm pending, but we're crossing over the lower low
                    if not self.position and self.data.Close[-1] < lh_val:
                        print_debug('Short')
                        #print_debug(self.data.df.tail(1))
                        self.sl = lh_val
                        print_debug(f'D: {self.data.df.index[-1]} O:{self.data.Open[-1]} H:{self.data.High[-1]} L:{self.data.Low[-1]} C:{self.data.Close[-1]} SL:{self.sl}')
                        self.sell(sl=self.sl)
                    elif self.position.is_long  and self.data.Close[-1] < lh_val:
                        print_debug('Short')
                        #print_debug(self.data.df.tail(1))
                        self.sl = lh_val
                        print_debug(f'D: {self.data.df.index[-1]} O:{self.data.Open[-1]} H:{self.data.High[-1]} L:{self.data.Low[-1]} C:{self.data.Close[-1]} SL:{self.sl}')
                        self.sell(sl=self.sl)
                    else:
                        #Else, update stoploss
                        if self.position and self.sl != lh_val:
                            self.sl = lh_val
                            print_debug(f'D: {self.data.df.index[-1]} SL: {self.sl}')


def get_data(symbol='CRUDEOIL1!', exchange='MCX', timeframe= convert_timeframe_to_quant('1m')):
    username = 'AnshulBot'
    password = '@nshulthakur123'
    
    tv = TvDatafeed(username, password)
    
    interval = timeframe
    
    
    #n_bars = int((14.5)*60/interval)+14
    n_bars = 1000
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
    return df


def get_data_from_file():
    df = pd.read_csv('./NIFTY50_1m.csv')
    #df = pd.read_csv('./NIFTY50_5m.csv')
    #df = pd.read_csv('./NIFTY50_15m.csv')
    #df = pd.read_csv('./NIFTY50_1h.csv')
    #df = pd.read_csv('./NIFTY50_1d.csv')
    
    for column in df.columns:
        if column != 'date':
            df[column] = pd.to_numeric(df[column])
    #df['date'] = pd.to_datetime(df['date'])
    #df['date'] = df['date'].tz_localize(None)
    df.set_index('date', inplace=True)
    df.index = pd.to_datetime(df.index)
    df.index = df.index.tz_localize(None)
    
    df = df.sort_index()
    df = df.reindex(columns = ['open', 'high', 'low', 'close', 'volume'])
    df.drop('volume', axis=1, inplace=True)
    
    return df

df = get_data()
df.rename(columns={"open": "Open", "high": "High", "low": "Low", "close":"Close", "volume":'Volume'}, inplace=True)

df = df[~df.index.duplicated(keep='first')]
#Optionally, filter out by date range
#print(df.head(10))
filter = True
if filter:
    start_date = '2022-08-12 09:00:00'
    end_date = '2022-08-12 10:00:00'
    df = df.loc[start_date:end_date]

print(df.head(1))
print(df.tail(1))

bt = Backtest(df, TrendFollow, cash=10000, commission=0.03/100, trade_on_close=False, exclusive_orders = True)
stats = bt.run()
print(stats)

#bt.plot()