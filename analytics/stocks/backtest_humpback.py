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
class HumpbackShorts(Strategy):
    '''
    A humpback is my hypothesized pattern resembling the second, lower top of an ABC correction wave
    The hypothesis is, by shorting once the price closes lower than the previous day's close, 
    one may earn decent profits as long as a strict stop loss is observed (end of timeframe basis)
    '''
    order = 1
    sl = 0
    tolerance = 2

    is_intraday = False
    #In case of intraday
    start_hr = 9
    start_min = 0
    start_offset = 5
    end_hr = 23
    end_min = 15 

    def init(self):
        # Precompute the two moving averages
        pass
    
    def next(self):
        hh_val = 0
        hl_val = 0
        lh_val = 0
        ll_val = 0

        hh_id = -1
        hl_id = -1
        lh_id = -1
        ll_id = -1
        if self.is_intraday:
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
                    print('Close position')
                    self.position.close()
                    return

        #Get the higher highs
        hh = getHigherHighs(self.data.Close, order=self.order)
        hh_idx = np.array([min(i[1] , len(self.data)-1) for i in hh])

        #Get the higher lows
        hl = getHigherLows(self.data.Close, order=self.order)
        hl_idx = np.array([min(i[1] , len(self.data)-1) for i in hl])

        #Get the lower lows
        ll = getLowerLows(self.data.Close, order=self.order)
        ll_idx = np.array([min(i[1] , len(df)-1) for i in ll])

        #Get the lower highs
        lh = getLowerHighs(self.data.Close, order=self.order)
        lh_idx = np.array([min(i[1] , len(df)-1) for i in lh])

        if hh_idx is not None and len(hh_idx)>1:
            hh_id = hh_idx[-1]-self.order
            if self.data.Close[hh_id]> hh_val and abs(hh_val - self.data.Close[hh_id])>self.tolerance:
                hh_val = self.data.High[hh_id]
        
        if hl_idx is not None and len(hl_idx)>1:        
            if self.data.Close[hl_idx[0]] < ll_val:
                #When trend changes, the last low won't be marked 
                ll_id = hl_idx[0]-self.order
                ll_val = self.data.Close[ll_id]
            hl_id = hl_idx[-1]-self.order
            if self.data.Close[hl_id] > hl_val and  abs(hl_val - self.data.Close[hl_id])>self.tolerance:
                hl_val = self.data.Close[hl_id]
        
        if ll_idx is not None and len(ll_idx)>1:
            ll_id = ll_idx[-1]-self.order
            if self.data.Close[ll_id] < ll_val and abs(ll_val - self.data.Close[ll_id])>self.tolerance:
                ll_val = self.data.Close[ll_id]
        
        if lh_idx is not None and len(lh_idx)>1:
            lh_id = lh_idx[-1]-self.order
            lh_val = df.close[lh_id]
            if self.data.Close[lh_idx[0]] > hh_val:
                hh_id = lh_idx[0]
                hh_val = self.data.Close[hh_id]
            lh_id = lh_idx[-1]
            if self.data.Close[lh_id] < lh_val and abs(lh_val - self.data.Close[lh_id])>self.tolerance:
                lh_val = self.data.Close[lh_id]
        else:
            lh_id = -1
        
        print(f'D: {self.data.df.index[-1]} HH:{hh}({hh_val}) HL:{hl}({hl_val}) LH:{lh}({lh_val}) LL:{ll}({ll_val}) ')
        if hh>ll and hl>ll:
            #Colud be uptrend
            if hh > hl:
                #uptrend confirm
                if not self.position  and self.data.Close[-1] > hl_val:
                    print('Long')
                    #print(self.data.df.tail(1))
                    self.sl = hl_val
                    print(f'D: {self.data.df.index[-1]} O:{self.data.Open[-1]} H:{self.data.High[-1]} L:{self.data.Low[-1]} C:{self.data.Close[-1]} SL:{self.sl}')
                    self.buy(sl = self.sl)
                elif self.position.is_short  and self.data.Close[-1] > hl_val:
                    print('Long')
                    #print(self.data.df.tail(1))
                    self.sl = hl_val
                    print(f'D: {self.data.df.index[-1]} O:{self.data.Open[-1]} H:{self.data.High[-1]} L:{self.data.Low[-1]} C:{self.data.Close[-1]} SL:{self.sl}')
                    self.buy(sl = self.sl)
                else:
                    if self.sl != hl_val:
                        self.sl = hl_val
                        print(f'D: {self.data.df.index[-1]} SL: {self.sl}')
                    #Else, update stop loss
            else:
                if hl > hh and self.data.Close[-1] > hh_val:
                    #uptrend pending confirm, but candle just closed above previous high
                    if not self.position and self.data.Close[-1] > hl_val:
                        print('Long')
                        #print(self.data.df.tail(1))
                        self.sl = hl_val
                        print(f'D: {self.data.df.index[-1]} O:{self.data.Open[-1]} H:{self.data.High[-1]} L:{self.data.Low[-1]} C:{self.data.Close[-1]} SL:{self.sl}')
                        self.buy(sl = self.sl)
                    elif self.position.is_short and self.data.Close[-1] > hl_val:
                        print('Long')
                        #print(self.data.df.tail(1))
                        self.sl = hl_val
                        print(f'D: {self.data.df.index[-1]} O:{self.data.Open[-1]} H:{self.data.High[-1]} L:{self.data.Low[-1]} C:{self.data.Close[-1]} SL:{self.sl}')
                        self.buy(sl = self.sl)
                    else:
                        if self.sl != hl_val:
                            self.sl = hl_val
                            print(f'D: {self.data.df.index[-1]} SL: {self.sl}')
                        #Else, update stop loss
        if lh > hh and lh > hl:
            #downtrend possible
            if ll > lh:
                #downtrend confirm
                if not self.position and self.data.Close[-1] < lh_val:
                    print('Short')
                    #print(self.data.df.tail(1))
                    self.sl = lh_val
                    print(f'D: {self.data.df.index[-1]} O:{self.data.Open[-1]} H:{self.data.High[-1]} L:{self.data.Low[-1]} C:{self.data.Close[-1]} SL:{self.sl}')
                    self.sell(sl=self.sl)
                elif self.position.is_long  and self.data.Close[-1] < lh_val:
                    print('Short')
                    #print(self.data.df.tail(1))
                    self.sl = lh_val
                    print(f'D: {self.data.df.index[-1]} O:{self.data.Open[-1]} H:{self.data.High[-1]} L:{self.data.Low[-1]} C:{self.data.Close[-1]} SL:{self.sl}')
                    self.sell(sl=self.sl)
                else:
                    #Else, update stoploss
                    if self.position and self.sl != lh_val:
                        self.sl = lh_val
                        print(f'D: {self.data.df.index[-1]} SL: {self.sl}')
            else:
                if lh > ll and self.data.Close[-1] < ll_val:
                    #downtrend confirm pending, but we're crossing over the lower low
                    if not self.position and self.data.Close[-1] < lh_val:
                        print('Short')
                        #print(self.data.df.tail(1))
                        self.sl = lh_val
                        print(f'D: {self.data.df.index[-1]} O:{self.data.Open[-1]} H:{self.data.High[-1]} L:{self.data.Low[-1]} C:{self.data.Close[-1]} SL:{self.sl}')
                        self.sell(sl=self.sl)
                    elif self.position.is_long  and self.data.Close[-1] < lh_val:
                        print('Short')
                        #print(self.data.df.tail(1))
                        self.sl = lh_val
                        print(f'D: {self.data.df.index[-1]} O:{self.data.Open[-1]} H:{self.data.High[-1]} L:{self.data.Low[-1]} C:{self.data.Close[-1]} SL:{self.sl}')
                        self.sell(sl=self.sl)
                    else:
                        #Else, update stoploss
                        if self.position and self.sl != lh_val:
                            self.sl = lh_val
                            print(f'D: {self.data.df.index[-1]} SL: {self.sl}')


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


if __name__ == "__main__":
    df = get_data()
    df.rename(columns={"open": "Open", "high": "High", "low": "Low", "close":"Close", "volume":'Volume'}, inplace=True)

    df = df[~df.index.duplicated(keep='first')]
    #Optionally, filter out by date range
    #print(df.head(10))
    filter = True
    if filter:
        start_date = '2020-01-01 09:00:00'
        end_date = '2022-12-15 10:00:00'
        df = df.loc[start_date:end_date]

    print(df.head(1))
    print(df.tail(1))

    bt = Backtest(df, HumpbackShorts, cash=10000, commission=0.03/100, trade_on_close=False, exclusive_orders = True)
    stats = bt.run()
    print(stats)

    #bt.plot()