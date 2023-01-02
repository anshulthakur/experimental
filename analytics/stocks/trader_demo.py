import os
import sys
import settings
import numpy as np
import pandas as pd

from stocks.models import Listing, Stock, Market
from lib.logging import set_loglevel, log
from lib.tradingview import convert_timeframe_to_quant, get_tvfeed_instance, Interval
from lib.retrieval import get_stock_listing
from lib.nse import NseIndia

import datetime
import time

import signal, os
from traceback import format_tb

def signal_handler(signum, frame):
    signame = signal.Signals(signum).name
    print(f'Signal handler called with signal {signame} ({signum})')
    dump_botinfo()
    exit(0)


class Position(object):
    def __init__(self, buy=None, sell=None, quantity=1):
        if buy is not None and sell is not None:
            log(f'Cannot have both buy and sell in a single order', 'error')
            raise Exception('Cannot have both buy and sell in a single order')
        self.open = True
        self.buy = buy
        self.sell = sell
        self.profit = 0
        self.quantity = quantity
    
    def is_long(self):
        return True if (self.buy is not None and self.sell is None) else False

    def close(self, price):
        if self.is_long():
            self.sell = price
        else:
            self.buy = price
        self.profit = (self.sell - self.buy)*self.quantity
        log(f'Closed position. Profit = {self.profit}', 'info')

class Broker(object):
    def __init__(self):
        super().__init__()
        pass

    def get_charges(self, buy=True, price=0, quantity=0, segment='equity'):
        if segment=='equity':
            brokerage = min(20, (0.03*price*quantity)/100)
            stt = ((0.025/100)*(price*quantity)) if buy is False else 0
            transaction = (0.00345/100)*(price*quantity)
            gst = (18/100)*(brokerage+transaction)
            stamp = ((0.003/100)*(price*quantity)) if buy is True else 0
            #print("Brokerage: {}\nSTT: {}\nTransaction Charges: {}\nGST:{}\nStamp Duty: {}".format(
            #       brokerage,stt,transaction,gst,stamp))
        elif segment=='options':
            brokerage = 20
            stt = ((0.05/100)*(price*quantity)) if buy is False else 0
            transaction = (0.053/100)*(price*quantity)
            gst = (18/100)*(brokerage+transaction)
            stamp = ((0.003/100)*(price*quantity)) if buy is True else 0
        elif segment=='commodity':
            quantity = quantity*100 #Commodities have a size of 100
            brokerage = min(20, (0.03*price*quantity)/100) 
            stt = ((0.05/100)*(price*quantity)) if buy is False else 0
            transaction = (0.05/100)*(price*quantity)
            gst = (18/100)*(brokerage+transaction)
            stamp = ((0.003/100)*(price*quantity)) if buy is True else 0
            print(brokerage+stt+transaction+gst+stamp)
        return(brokerage+stt+transaction+gst+stamp)
    
    def get_profit(self, quantity=0, buy=0, sell=0, segment='equity'):
        buy_side = self.get_charges(buy=True, price=buy, quantity=quantity, segment = segment)
        sell_side = self.get_charges(buy=False, price=sell, quantity=quantity, segment = segment)
        gross_profit = (sell-buy)*quantity
        return(gross_profit - buy_side - sell_side)
    
    def get_break_even_profit_margin(self, quantity=0, buy=None, sell=None, segment='equity'):
        #Approximate margin (by doubling the one side brokerage)
        if buy is not None:
            #Long
            margin = 2* self.get_charges(buy=True, price=buy if buy != None else 0, quantity=quantity, segment = segment)
        elif sell is not None:
            #Short
            margin = 2* self.get_charges(buy=False, price=sell if sell!=None else 0, quantity=quantity, segment = segment)
        else:
            print("Neither buy nor sell. Kehna kya chahte ho?")
            return -1
        return margin

class BaseBot(object):
    def __init__(self, cash=0, lot_size=1):
        self.orderbook = []
        self.position = None
        self.charges = 0
        self.cash = cash
        self.initial_cash = cash
        self.lot_size = lot_size
        super().__init__()

    def buy(self, price, date=datetime.datetime.now()):
        if self.position is not None:
            if self.position.is_long():
                log(f'Already long', 'warning')
                pass
            else:
                log(f'Close shorts', 'info')
                self.position.close(price)
                charges = self.get_charges(segment='options', quantity=self.lot_size, buy=True, price=price)
                self.cash = self.cash + (price*self.lot_size) - charges
                self.charges += charges
                self.orderbook.append({'timestamp': date, 
                                       'operation': 'buy', 
                                       'price': price, 
                                       'quantity': self.lot_size, 
                                       'charges': charges})
                #log(f'{self.orderbook[-1]}', 'info')
                log(f'Cash: {self.cash} Charges: {self.charges}', 'info')
                self.position = None
        else:
            charges = self.get_charges(segment='options', quantity=self.lot_size, buy=True, price=price)
            if self.cash > ((price*self.lot_size) + charges):
                self.position = Position(buy=price, quantity=self.lot_size)
                self.cash = self.cash - (price*self.lot_size) - charges
                self.charges += charges
                self.orderbook.append({'timestamp': date, 
                                       'operation': 'buy', 
                                       'price': price, 
                                       'quantity': self.lot_size, 
                                       'charges': charges})
                #log(f'{self.orderbook[-1]}', 'info')
                log(f'Cash: {self.cash} Charges: {self.charges}')
            else:
                log(f'Not enough cash', 'warning')

    def sell(self, price, date=datetime.datetime.now()):
        if self.position is not None:
            if not self.position.is_long():
                log(f'Already short', 'warning')
                pass
            else:
                log(f'Close longs', 'info')
                self.position.close(price)
                charges = self.get_charges(segment='options', quantity=self.lot_size, buy=False, price=price)
                self.cash = self.cash + (price*self.lot_size) - charges
                self.charges += charges
                self.orderbook.append({'timestamp': date, 
                                       'operation': 'sell', 
                                       'price': price, 
                                       'quantity': self.lot_size, 
                                       'charges': charges})
                #log(f'{self.orderbook[-1]}', 'info')
                log(f'Cash: {self.cash} Charges: {self.charges}')
                self.position = None
        else:
            charges = self.get_charges(segment='options', quantity=self.lot_size, buy=False, price=price)
            if self.cash > ((price*self.lot_size) + charges):
                self.position = Position(sell=price, quantity=self.lot_size)
                self.cash = self.cash - (price*self.lot_size) - charges
                self.charges += charges
                self.orderbook.append({'timestamp': date, 
                                       'operation': 'sell', 
                                       'price': price, 
                                       'quantity': self.lot_size, 
                                       'charges': charges})
                #log(f'{self.orderbook[-1]}', 'info')
                log(f'Cash: {self.cash} Charges: {self.charges}')
            else:
                log(f'Not enough cash', 'warning')

    def close_position(self, price, date=datetime.datetime.now()):
        if self.position is not None:
            if self.position.is_long():
                self.position.close(price)
                charges = self.get_charges(segment='options', 
                                            quantity=self.lot_size, 
                                            buy=False, price=price)
                self.cash = self.cash + (price*self.lot_size) - charges
                self.charges += charges
                self.orderbook.append({'timestamp': date, 
                                       'operation': 'sell', 
                                       'price': price, 
                                       'quantity': self.lot_size, 
                                       'charges': charges})
                #log(f'{self.orderbook[-1]}', 'info')
                log(f'Cash: {self.cash} Charges: {self.charges}')
            else:
                self.position.close(price)
                charges = self.get_charges(segment='options', quantity=self.lot_size, buy=True, price=price)
                self.cash = self.cash + (price*self.lot_size) - charges
                self.charges += charges
                self.orderbook.append({'timestamp': date, 
                                       'operation': 'buy', 
                                       'price': price, 
                                       'quantity': self.lot_size, 
                                       'charges': charges})
                #log(f'{self.orderbook[-1]}', 'info')
                log(f'Cash: {self.cash} Charges: {self.charges}')
        self.position = None

class LongBot(BaseBot, Broker):
    #In case of intraday
    start_hr = 9
    start_min = 0
    start_offset = 5
    end_hr = 15
    end_min = 15

    def __init__(self, cash=0, lot_size=1, timeframe=None):
        self.timeframe = timeframe
        self.sl = 0
        self.resistances = []
        self.supports = []
        self.direction = 'UNKNOWN'

        self.is_intraday = True
        super().__init__(cash, lot_size)

    def next(self, df):
        if self.is_intraday:
            d = df.index[-1].to_pydatetime()
            if d.hour==self.end_hr and d.minute>=self.end_min:
                if self.position:
                    log('End of day.', 'info')
                    self.close_position(df['Close'][-1], date=df.index[-1].to_pydatetime())
                    log(f'Close position {df["Close"][-1]}. Total Charges (so far): {self.charges}', 'info')
                    return
                else:
                    return
        if len(df) <= 1:
            #log(f"First candle. Supports: {self.supports}. Resistances: {self.resistances}")
            return
        if self.position:
            if self.sl > df['Close'][-1]:
                self.close_position(df['Close'][-1], date=df.index[-1].to_pydatetime())
                log(f'SL Hit {df["Close"][-1]}. Total Charges (so far): {self.charges}', 'info')
        if df['Close'][-1] > df['Close'][-2]:
            if len(self.supports)==0:
                #Moving up for the first time. Mark support
                self.supports.append((df.index[-2], df['Close'][-2]))
                self.direction = 'UP'
                log(f'New support: {self.supports[-1]}', 'debug')
            elif (len(self.resistances)>0) and (self.direction == 'UP') and (df['Close'][-1] > self.resistances[-1][1]):
                #Crossed resistance. Mark it as support
                last_resistance = self.resistances.pop()
                self.supports.append(last_resistance)
                log(f'New resistance turned support: {self.supports[-1]}', 'debug')
                if not self.position:
                    #If we don't have any position, then go long here
                    log(f"Go long: {df['Close'][-1]}", 'info')
                    self.sl = self.supports[-2][1]
                    self.buy(df['Close'][-1], date=df.index[-1].to_pydatetime())
                    
                else:
                    #Else, update Stop Loss
                    self.sl = self.supports[-2][1]
                    log(f'Update stop loss: {self.sl}', 'info')
            elif (self.direction == 'DOWN'):
                #Could be turning around, mark support
                self.supports.append((df.index[-2], df['Close'][-2]))
                self.direction = 'UP'
                log(f'New support: {self.supports[-1]}', 'debug')
        elif df['Close'][-1] < df['Close'][-2]:
            if len(self.supports)==0:
                #We haven't found first support. Opening downtrend.
                #Wait until we get a prospective uptrend
                return
            elif self.direction == 'UP':
                #Price changed direction, mark resistance
                self.resistances.append((df.index[-2], df['Close'][-2]))
                self.direction = 'DOWN'
                log(f'New resistance: {self.resistances[-1]}', 'debug')
            elif (self.direction == 'DOWN') and (df['Close'][-1] < self.supports[-1][1]):
                #Downtrend broke support, mark it as resistance now
                last_support = self.supports.pop()
                self.resistances.append(last_support)
                log(f'New resistance from support: {self.resistances[-1]}', 'debug')

def get_tradebook():
    global tradebook
    if tradebook is None:
        tradebook = {}
    
    return tradebook

tradebook = None
bots = {'1m': {
                    'bot': LongBot(cash=20000000, lot_size=75, timeframe='1m'),
                    'resampler': None,
                    'scheduled': True,
                    'last_run': None
                  },
            '5m': {
                    'bot': LongBot(cash=20000000, lot_size=75, timeframe='5m'),
                    'resampler': '5Min',
                    'scheduled': True,
                    'last_run': None
                  },
            '15m': {
                    'bot': LongBot(cash=20000000, lot_size=75, timeframe='15m'),
                    'resampler': '15Min',
                    'scheduled': True,
                    'last_run': None
                  },
            '30m': {
                    'bot': LongBot(cash=20000000, lot_size=75, timeframe='30m'),
                    'resampler': '30Min',
                    'scheduled': True,
                    'last_run': None
                  },
            '60m': {
                    'bot': LongBot(cash=20000000, lot_size=75, timeframe='60m'),
                    'resampler': '60Min',
                    'scheduled': True,
                    'last_run': None
                  },
            }
            

def get_dataframe_yahoo(stock, market, timeframe, date):
    import yfinance as yf

    s = yf.Ticker(stock)
    df = s.history(period="1d", interval=timeframe)

    #log(df.head(10), 'info')
    return df

def get_dataframe_lib(stock, market, timeframe, date, duration=60, online=True):
    if 'w' in timeframe.lower():
        duration = duration * 5 
    if 'M' in timeframe:
        duration = duration * 25

    if online or timeframe.strip()[-1] not in ['d', 'w', 'M']:
        #Either we're explicitly told to fetch data from TV, or timeframe is shorter than a day
        username = 'AnshulBot'
        password = '@nshulthakur123'

        tv = get_tvfeed_instance(username, password)
        interval=convert_timeframe_to_quant(timeframe)

        symbol = stock.strip().replace('&', '_')
        symbol = symbol.replace('-', '_')
        nse_map = {'UNITDSPR': 'MCDOWELL_N',
                    'MOTHERSUMI': 'MSUMI'}
        if symbol in nse_map:
            symbol = nse_map[symbol]
        log(f'Symbol: {stock}, Exchange: {market}, TF: {interval}, Duration: {duration}', 'info')
        s_df = tv.get_hist(
                            symbol,
                            market,
                            interval=interval,
                            n_bars=duration,
                            extended_session=False,
                        )
        if (s_df is None) or (s_df is not None and len(s_df)==0):
            log('Skip {}'.format(symbol), logtype='warning')
            pass
    else:
        s_df = get_stock_listing(stock, duration=duration, last_date = date, 
                                    resample=True if timeframe[-1].lower() in ['w', 'm'] else False, 
                                    monthly=True if 'm' in timeframe.lower() else False)
        s_df = s_df.drop(columns = ['delivery', 'trades'])
        if len(s_df)==0:
            log('Skip {}'.format(stock.symbol), logtype='warning')
    s_df.rename(columns={"open": "Open", "high": "High", "low": "Low", "close":"Close", "volume":'Volume'}, inplace=True)
    return s_df

def get_dataframe(stock, market, timeframe, date, duration=60, online=True, use_yahoo=True):
    if use_yahoo:
        s_df = get_dataframe_yahoo(stock=stock, 
                                 market=market, 
                                 timeframe=timeframe, 
                                 date=date)
    else:
        s_df = get_dataframe_lib(stock=stock, 
                                 market=market, 
                                 timeframe=timeframe, 
                                 date=date, 
                                 online=online, 
                                 duration=duration)
    return s_df

def dump_botinfo():
    global bots
    for bot in bots:
        profit = ((bots[bot]["bot"].cash - bots[bot]["bot"].initial_cash)/bots[bot]["bot"].initial_cash)*100
        print(f'TF {bot}:')
        print(f'\tFinal capital:\t{bots[bot]["bot"].cash}\t({profit}%)')
        print(f'\tTrades:\t{len(bots[bot]["bot"].orderbook)}')
        print(f'\tCharges: {bots[bot]["bot"].charges}')

    for bot in bots:
        print(f'TF {bot}: Orderbook')
        for order in bots[bot]["bot"].orderbook:
            print(order)

def main(backtest=False):
    global bots
    # Set the signal handler and a 5-second alarm
    signal.signal(signal.SIGINT, signal_handler)

    logic = {'Open'  : 'first',
             'High'  : 'max',
             'Low'   : 'min',
             'Close' : 'last'}
    
    
    if backtest:
        df_store = {}
        for bot in bots:
            # df_store[bot] = {'df' :get_dataframe(stock='^NSEI', 
            #                                      market='NSE', 
            #                                      timeframe=bot, 
            #                                      online=True, 
            #                                      use_yahoo=True, 
            #                                      date=None),
            #                 'index': 1,
            #                 }
            duration = (60*6)+15+1 #Prepare for minutewise scan
            if bot[-1] is 'm':
                duration = (duration//int(bot[0:-1])) + 1
            elif bot[-1] is 'h':
                duration = (duration//60) + 1
            log(f'Duration: {duration}', 'info')
            df_store[bot] = {'df' :get_dataframe(stock='NIFTY', 
                                                 market='NSE', 
                                                 timeframe=bot, 
                                                 online=True, 
                                                 use_yahoo=False, 
                                                 date=None,
                                                 duration=duration),
                            'index': 1,
                            }
        for bot in bots:
            log(f'Running [{bot}]: Index {df_store[bot]["index"]}', 'info')
            while df_store[bot]['index']<=len(df_store[bot]['df']) and bots[bot]['scheduled']:
                s_df = df_store[bot]['df'].iloc[0:df_store[bot]['index']]
                if len(s_df)>1 and s_df.index[-2].to_pydatetime().day != datetime.datetime.today().day:
                    log('Skip stale entries', 'info')
                elif len(s_df)==1 and s_df.index[-1].to_pydatetime().day != datetime.datetime.today().day:
                    log('Skip stale entries', 'info')
                elif len(s_df)>0:
                    #log(s_df.tail(1), 'info')
                    bots[bot]['bot'].next(s_df)
                df_store[bot]['index'] +=1

    else:
        nseObj = NseIndia(legacy=True)
        try:
            # using now() to get current time
            current_time = datetime.datetime.now()
            #Sleep to align running to near start of minute
            time.sleep(60 - current_time.second + 2)
            last_minute = current_time.minute+1
            while (current_time.hour < 15 or (current_time.hour > 15 and current_time.minute<30)):
                #log(f'\nRun loop once: ', 'info')
                
                #df = get_dataframe(stock='^NSEI', exchange='NSE', timeframe='1m', online=True, use_yahoo=True)
                df = nseObj.getIndexIntradayData(index='NIFTY 50', resample=None)
                for bot in bots:
                    #Run only if an epoch has elapsed
                    if (int(bot[0:-1])<60 and ((current_time.minute > 0) and (current_time.minute-1)%int(bot[0:-1])==0)) or \
                        ((bots[bot]['last_run'] is not None) and (int(bot[0:-1])==60 and current_time.hour > bots[bot]['last_run'].hour)) :
                        log(f'\nTF {bot} scheduled', 'info')
                        bots[bot]['scheduled'] = True

                    if bots[bot]['scheduled']:
                        #s_df = get_dataframe(stock='^NSEI', market='NSE', timeframe=bot, online=True, use_yahoo=True, date=None)
                        #s_df = get_dataframe(stock='NIFTY', market='NSE', timeframe=bot, online=True, use_yahoo=False, date=None)
                        s_df = df.resample(bot+'in').ohlc() #make it 'min' (for minute)
                        s_df = s_df.droplevel(level=0, axis=1)
                        s_df.rename(columns={"open": "Open", "high": "High", "low": "Low", "close":"Close"}, inplace=True)
                        if s_df.index[-2].to_pydatetime().day != datetime.datetime.today().day:
                            log('Skip stale entries', 'info')
                        else:
                            bots[bot]['scheduled'] = False
                            bots[bot]['last_run'] = current_time
                            # if bots[bot]['resampler'] is not None:
                            #     s_df = df.resample(bots[bot]['resampler']).apply(logic)
                            # else:
                            #     s_df = df
                            #log(f'Running', 'info')
                            bots[bot]['bot'].next(s_df)
                #Done processing, now fetch next candle or wait until next minute
                last_minute = current_time.minute
                current_time = datetime.datetime.now()
                if current_time.minute > last_minute:
                    time.sleep(60 - current_time.second)
        except:
            log('Exception', 'error')
            tb_type = sys.exc_info()[0]
            tb_value = sys.exc_info()[1]
            tb_info = format_tb(sys.exc_info()[2])
            tb_output = ''
            for line in tb_info:
                tb_output += line
            log(f"Exception: {tb_type}\n{tb_value}\n{tb_output}", "error")
            pass
    dump_botinfo()

if __name__ == "__main__":
    set_loglevel('debug')
    import argparse
    parser = argparse.ArgumentParser(description='Scan stock securities for trend')
    parser.add_argument('-s', '--stock', help="Stock code")
    parser.add_argument('-e', '--exchange', help="Exchange")
    parser.add_argument('-t', '--timeframe', help="Timeframe(s). If specifying more than one, separate using commas")
    parser.add_argument('-d', '--date', help="Date")
    parser.add_argument('-l', '--list', help="List of stocks to scan (MARKET:SYMBOL)")
    parser.add_argument('-o', '--online', action='store_true', default=False, help="Online mode:Fetch from tradingview")
    parser.add_argument('-b', '--backtest', help="Backtest mode", action="store_true", default=False)
    args = parser.parse_args()

    main(args.backtest)
