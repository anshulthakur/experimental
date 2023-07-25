import os
import sys
import init
import numpy as np
import pandas as pd

from stocks.models import Listing, Stock, Market
from lib.retrieval import get_stock_listing
from lib.pivots import getHigherHighs, getLowerHighs, getLowerLows, getHigherLows
from lib.logging import set_loglevel, log
from lib.tradingview import convert_timeframe_to_quant, get_tvfeed_instance, Interval

#Plotting
import matplotlib
import matplotlib.pyplot as plt
from mplfinance.original_flavor import candlestick_ohlc
import matplotlib.dates as mpl_dates
from matplotlib.dates import date2num

import talib
from talib.abstract import *
from talib import MA_Type

from backtesting import Strategy
from backtesting import Backtest

from lib.pivots import getHHIndex, getHLIndex, getLLIndex, getLHIndex

#Create a strategy
class LongerBot(Strategy):
    # This is a bot that only goes long when some conditions are met
    # The principle is to track dyamic resistances and supports and act when price
    # crosses those values at various timeframes
    sl = 0
    brokerage = 20

    charges = 0
    is_intraday = True

    #In case of intraday
    start_hr = 9
    start_min = 0
    start_offset = 5
    end_hr = 15
    end_min = 15 
    
    
    def init(self):
        # Precompute the two moving averages
        self.resistances = []
        self.supports = []
        self.direction = 'UNKNOWN'
        pass
    
    def next(self):
        if self.is_intraday:
            d = self.data.df.index[-1].to_pydatetime()
            if d.hour==self.end_hr and d.minute==self.end_min:
                if self.position:
                    self.position.close()
                    self.charges += 100
                    log(f'Close position. Total Charges (so far): {self.charges}', 'info')
                    return
        if len(self.data.Close) <= 1:
            return
        if self.position:
            if self.sl > self.data.Close[-1]:
                self.position.close()
                self.charges += 100
                log(f'SL Hit. Total Charges (so far): {self.charges}', 'info')
        if self.data.Close[-1] > self.data.Close[-2]:
            if len(self.supports)==0:
                #Moving up for the first time. Mark support
                self.supports.append((self.data.df.index[-2], self.data.Close[-2]))
                self.direction = 'UP'
                log(f'New support: {self.supports[-1]}', 'debug')
            elif (len(self.resistances)>0) and (self.direction == 'UP') and (self.data.Close[-1] > self.resistances[-1][1]):
                #Crossed resistance. Mark it as support
                last_resistance = self.resistances.pop()
                self.supports.append(last_resistance)
                log(f'New resistance turned support: {self.supports[-1]}', 'debug')
                if not self.position:
                    #If we don't have any position, then go long here
                    log('Go long', 'info')
                    self.sl = self.supports[-2][1]
                    self.buy()
                    
                else:
                    #Else, update Stop Loss
                    log('Update stop loss', 'info')
                    self.sl = self.supports[-2][1]
            elif (self.direction == 'DOWN'):
                #Could be turning around, mark support
                self.supports.append((self.data.df.index[-2], self.data.Close[-2]))
                self.direction = 'UP'
                log(f'New support: {self.supports[-1]}', 'debug')
        elif self.data.Close[-1] < self.data.Close[-2]:
            if len(self.supports)==0:
                #We haven't found first support. Opening downtrend.
                #Wait until we get a prospective uptrend
                return
            elif self.direction == 'UP':
                #Price changed direction, mark resistance
                self.resistances.append((self.data.df.index[-2], self.data.Close[-2]))
                self.direction = 'DOWN'
                log(f'New resistance: {self.resistances[-1]}', 'debug')
            elif (self.direction == 'DOWN') and (self.data.Close[-1] < self.supports[-1][1]):
                #Downtrend broke support, mark it as resistance now
                last_support = self.supports.pop()
                self.resistances.append(last_support)
                log(f'New resistance from support: {self.resistances[-1]}', 'debug')

def get_data_from_file():
    #df = pd.read_csv('./NIFTY50_1m.csv')
    #df = pd.read_csv('./NIFTY50_5m.csv')
    #df = pd.read_csv('./NIFTY50_15m.csv')
    #df = pd.read_csv('./NIFTY50_30m.csv')
    df = pd.read_csv('./NIFTY50_1h.csv')
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

if __name__ == "__main__":
    set_loglevel('info')
    df = get_data_from_file()
    df.rename(columns={"open": "Open", "high": "High", "low": "Low", "close":"Close", "volume":'Volume'}, inplace=True)

    df = df[~df.index.duplicated(keep='first')]
    #Optionally, filter out by date range
    #print(df.head(10))
    filter = False
    if filter:
        start_date = '2020-01-01 09:00:00'
        end_date = '2022-12-15 10:00:00'
        df = df.loc[start_date:end_date]

    print(df.head(1))
    print(df.tail(1))

    bt = Backtest(df, LongerBot, cash=100000, commission=0.00/100, trade_on_close=False, exclusive_orders = True)
    stats = bt.run()
    print(stats)

    #bt.plot()