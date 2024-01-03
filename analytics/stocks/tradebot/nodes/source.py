from tradebot.base import FlowGraphNode
from lib.logging import log

from lib.nse import NseIndia
from tradebot.base.signals import EndOfData, EndOfDay
import pandas as pd
import json
import os

import datetime
from dateutil.relativedelta import relativedelta

from lib.indices import get_index_members, load_index_members

from lib.tradingview import convert_timeframe_to_quant, get_tvfeed_instance, Interval
from lib.cache import cached

class SourceNode(FlowGraphNode):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def next(self, connection, **kwargs):
        pass

class TradingViewSource(SourceNode):
    def __init__(self, symbol=None, exchange='NSE', timeframe='1d', **kwargs):
        super().__init__(signals= [EndOfData], publications=[self.timeframe], **kwargs)
        self.timeframe = convert_timeframe_to_quant(timeframe)
        username = 'AnshulBot'
        password = '@nshulthakur123'
        self.tv = get_tvfeed_instance(username, password)
        self.exchange = exchange
        if symbol is None:
            raise Exception(f"Symbol must not be None")
        self.symbol = symbol.strip().replace('&', '_').replace('-', '_')
        self.duration = 500

        nse_map = {'UNITDSPR': 'MCDOWELL_N',
                    'MOTHERSUMI': 'MSUMI'}
        if self.symbol in nse_map:
            self.symbol = nse_map[self.symbol]
        log(f'Symbol: {self.symbol}, Exchange: {self.exchange}, TF: {self.timeframe.value}, Duration: {self.duration}', 'debug')
        self.df = None

        self.last_ts = None #Last index served
        self.index = None
        self.ended = False

    async def next(self, connection, **kwargs):
        #log(f'{self}: {kwargs}', 'debug')
        if not self.ready(connection, **kwargs):
            log(f'{self}: Not ready yet', 'debug')
            return
        if self.ended:
            return
        if self.df is None:
            self.df = self.tv.get_hist(
                            self.symbol,
                            self.exchange,
                            interval=self.timeframe,
                            n_bars=self.duration,
                            extended_session=False,
                        )
            if (self.df is None) or (self.df is not None and len(self.df)==0):
                log('Skip {}'.format(self.symbol), logtype='warning')
                return
            self.df.index = self.df.index + pd.DateOffset(hours=17, minutes=45)
        elif self.mode in ['buffered', 'stream']:
            log('Re-fetch data', 'debug')
            self.df = self.tv.get_hist(
                            self.symbol,
                            self.exchange,
                            interval=self.interval,
                            n_bars=self.duration,
                            extended_session=False,
                        )
            self.df = self.df.loc[self.last_ts:].copy()
        
        if self.mode in ['backtest', 'buffered']:
            if self.index is None:
                self.index = 0
            if self.index < len(self.df):
                df = self.df.iloc[0:self.index+1].copy()
                if len(df)>0:
                    self.last_ts = df.index[-1]
                    #log(f'{df.tail(1)}', 'debug')
                    for node,connection in self.connections:
                        await node.next(connection=connection, data = df.copy(deep=True))
                    await self.notify(df)
                    self.consume()
                else:
                    log('No data to pass', 'debug')
                    return
                self.index +=1
            else:
                if not self.ended:
                    await self.emit(EndOfData(timestamp=self.df.index[-1]))
                    self.ended = True
                return
        else:
            if self.last_ts == None:
                self.last_ts = self.df.index[-1]
                for node,connection in self.connections:
                    await node.next(connection=connection, data = self.df.copy(deep=True))
                await self.notify(df)
                self.consume()

class DbSource(SourceNode):
    def __init__(self, symbol, exchange, timeframe, **kwargs):
        super().__init__(**kwargs)


class YahooSource(SourceNode):
    def __init__(self, symbol, timeframe, **kwargs):
        super().__init__(**kwargs)

class NseSource(SourceNode):

    def __init__(self, symbol, timeframe, is_index=True, **kwargs):
        super().__init__(signals= [EndOfData], publications=[self.timeframe], **kwargs)
        self.timeframe = self.sanitize_timeframe(timeframe)
        self.symbol = symbol

        self.source = NseIndia(timeout=10, legacy=True)
        self.df = None
        self.last_ts = None #Last index served
        self.index = None
        self.ended = False

    async def next(self, connection=None, **kwargs):
        #log(f'{self}: {kwargs}', 'debug')
        if not self.ready(connection, **kwargs):
            log(f'{self}: Not ready yet', 'debug')
            return
        if self.ended:
            return
        if self.df is None:
            log('Fetch data', 'debug')
            self.df = self.source.getIndexIntradayData(index='NIFTY 50', resample=self.timeframe)
        elif self.mode in ['buffered', 'stream']:
            log('Re-fetch data', 'debug')
            self.df = self.source.getIndexIntradayData(index='NIFTY 50', resample=self.timeframe)
            self.df = self.df.loc[self.last_ts:].copy()
        if self.mode in ['backtest', 'buffered']:
            if self.index is None:
                self.index = 0
            if self.index < len(self.df):
                df = self.df.iloc[0:self.index+1].copy()
                if len(df)>0:
                    self.last_ts = df.index[-1]
                    #log(f'{df.tail(1)}', 'debug')
                    for node,connection in self.connections:
                        await node.next(connection=connection, data = df.copy(deep=True))
                    await self.notify(df)
                    self.consume()
                else:
                    log('No data to pass', 'debug')
                    return
                self.index +=1
            else:
                if not self.ended:
                    await self.emit(EndOfData(timestamp=self.df.index[-1]))
                    self.ended = True
                return
        else:
            if self.last_ts == None:
                self.last_ts = self.df.index[-1]
                for node,connection in self.connections:
                    await node.next(connection=connection, data = self.df.copy(deep=True))
                await self.notify(df)
                self.consume()

class NseMultiStockSource(SourceNode):
    '''
    Fetch the latest price for TOTAL MARKET INDEX members in a single API call,
    append it to the previous df and pass the df
    '''
    def __init__(self, timeframe='1D', is_index=True, offline=False, offset = 0, **kwargs):
        super().__init__(signals= [EndOfData], **kwargs)
        self.offline = offline
        self.timeframe = self.sanitize_timeframe(timeframe)
        if self.timeframe[-1] in ['m', 's', 'h', 'H']:
            self.offline = False #Override
        self.source = None
        self.df = None
        self.last_ts = None #Last index served
        self.index = None
        self.ended = False
        self.offset = offset
        #super().__init__(**kwargs)

    async def next(self, connection=None, **kwargs):
        if self.source is None and self.mode != 'backtest':
            self.source = NseIndia(timeout=10, legacy=False)
        #log(f'{self}: {kwargs}', 'debug')
        if not self.ready(connection, **kwargs):
            log(f'{self}: Not ready yet', 'debug')
            return
        if self.ended:
            return
        if self.df is None:
            log('Fetch data', 'debug')
            if self.mode in ['backtest', 'buffered']:
                #Get previous data on this timeframe from DB or tradingview
                members = get_index_members(name='NIFTY TOTAL MARKET')
                self.df = load_index_members(sector='NIFTY TOTAL MARKET', 
                                        members=members,
                                        entries=295,
                                        interval=convert_timeframe_to_quant(self.timeframe),
                                        online=not self.offline)
                self.df.fillna(0, inplace=True)
                #log(self.df.head(1), 'debug')
                #log(self.df.tail(1), 'debug')
                #log(self.df.tail(1).isnull().sum().sum(), 'debug')
                #nan_cols = self.df.tail(1)[self.df.tail(1).columns[self.df.tail(1).isnull().any()]]
                #log(nan_cols, 'debug')
            else:
                self.df = self.source.getEquityStockIndices(index='NIFTY TOTAL MARKET')
                self.df.fillna(0, inplace=True)
                #log(self.df.head(10), 'debug')
                #self.df['datetime'] = kwargs.get('data')
                #self.df.set_index('datetime', inplace=True)
                #self.df.sort_index(inplace=True)
        else:
            if self.mode not in ['backtest', 'buffered']:
                log('Re-fetch data', 'debug')
                df = self.source.getEquityStockIndices(index='NIFTY TOTAL MARKET')
                #df['datetime'] = kwargs.get('data')
                #df.set_index('datetime', inplace=True)
                #df.sort_index(inplace=True)
                #log(df.tail(10), 'debug')
                self.df = pd.concat([self.df, df], join='outer', sort=True)
                self.df.drop_duplicates(inplace=True)
                #self.df = self.df.loc[self.last_ts:].copy()
        #log(self.df.tail(1), 'debug')
        #log(len(self.df), 'debug')
        if self.index is None:
            self.index = self.offset
        if self.index < len(self.df):
            df = self.df.iloc[0:self.index+1].copy()
            if len(df)>0:
                self.last_ts = df.index[-1]
                #log(f'{df.tail(1)}', 'debug')
                for node,connection in self.connections:
                    await node.next(connection=connection, data = df.copy(deep=True))
                self.consume()
            else:
                log('No data to pass', 'debug')
                self.consume()
                return
            self.index +=1
        else:
            if not self.ended:
                await self.emit(EndOfData(timestamp=self.df.index[-1]))
                self.ended = True
            return



class MultiStockSource(SourceNode):
    '''
    Fetch the latest price for a list of scripts in a single API call,
    append it to the previous df and pass the df
    '''

    def __init__(self, member_file=None, timeframe='1D', offline=False, offset = 0, min_entries= 200, **kwargs):
        super().__init__(signals= [EndOfData], **kwargs)
        self.memberfile = member_file
        if self.memberfile is None:
            raise Exception("Member File must be specified")
        if not os.path.exists('runtime/lists/'+self.memberfile):
            raise Exception("Member File must exist in runtime/lists/")
        self.offline = offline
        self.timeframe = self.sanitize_timeframe(timeframe)
        if self.timeframe[-1] in ['m', 's', 'h', 'H']:
            self.offline = False #Override
        self.df = None
        self.last_ts = None #Last index served
        self.index = None
        self.ended = False
        self.offset = offset
        self.min_entries = min_entries
        self.drop_columns = kwargs.get('drop_columns', ['open', 'high', 'low', 'volume'])
        #super().__init__(**kwargs)

    def get_members(self, name):
        members = []
        stocks = []
        with open(f'runtime/lists/{name}', 'r', newline='') as fd:
            reader = json.load(fd)
            for market in reader:
                if market.lower() not in ['nse', 'bse']:
                    continue
                for member in reader[market]:
                    if member not in stocks: #Don't have repeated names if they are listed on BSE and NSE
                        stocks.append(member)
                        members.append(f"{market.upper().strip()}:{member.upper().strip()}")
        return members

    async def next(self, connection=None, **kwargs):
        if not self.ready(connection, **kwargs):
            log(f'{self}: Not ready yet', 'debug')
            return
        if self.ended:
            return
        if self.df is None:
            log('Fetch data', 'debug')
            if self.mode in ['backtest', 'buffered']:
                #Get previous data on this timeframe from DB or tradingview
                members = self.get_members(name=self.memberfile)
                self.df = load_index_members(sector=self.memberfile, 
                                        members=members,
                                        entries=max(self.offset, self.min_entries),
                                        interval=convert_timeframe_to_quant(self.timeframe),
                                        online=not self.offline,
                                        date = datetime.date.today(),
                                        drop_columns=self.drop_columns)
                self.df.fillna(0, inplace=True)
                #log(self.df.head(10), 'debug')
                #log(self.df.tail(10), 'debug')
                #log(f'Length: {len(self.df)}')
                #log(self.df.tail(1).isnull().sum().sum(), 'debug')
                #nan_cols = self.df.tail(1)[self.df.tail(1).columns[self.df.tail(1).isnull().any()]]
                #log(nan_cols, 'debug')
            else:
                self.df = self.source.getEquityStockIndices(index='NIFTY TOTAL MARKET')
                self.df.fillna(0, inplace=True)
                #log(self.df.head(10), 'debug')
                #self.df['datetime'] = kwargs.get('data')
                #self.df.set_index('datetime', inplace=True)
                #self.df.sort_index(inplace=True)
        else:
            if self.mode not in ['backtest', 'buffered']:
                log('Re-fetch data', 'debug')
                df = self.source.getEquityStockIndices(index='NIFTY TOTAL MARKET')
                #df['datetime'] = kwargs.get('data')
                #df.set_index('datetime', inplace=True)
                #df.sort_index(inplace=True)
                #log(df.tail(1), 'debug')
                self.df = pd.concat([self.df, df], join='outer', sort=True)
                self.df.drop_duplicates(inplace=True)
                #self.df = self.df.loc[self.last_ts:].copy()
        #log(self.df.tail(1), 'debug')
        #log(len(self.df), 'debug')
        if self.index is None:
            self.index = self.offset if self.offset >=0 else len(self.df)+self.offset
        if self.index < len(self.df):
            df = self.df.iloc[0:self.index+1].copy()
            if len(df)>0:
                self.last_ts = df.index[-1]
                log(f'{self.name}:{df.tail(1)}', 'debug')
                for node,connection in self.connections:
                    await node.next(connection=connection, data = df.copy(deep=True), metadata={'timeframe': self.timeframe})
                self.consume()
            else:
                log('No data to pass', 'debug')
                self.consume()
                return
            self.index +=1
        else:
            if not self.ended:
                await self.emit(EndOfData(timestamp=self.df.index[-1]))
                self.ended = True
            return


class FolderSource(SourceNode):
    '''
    Read from the folder organized as:
    - YEAR
        - Month
            - Day
                - Stock.csv
    '''
    def __init__(self, folder, symbol='NIFTY50', start_date = '2012-01-01 09:15', market_start_time='09:15:00', market_end_time='15:15:00', timeframe='1min', offset=0, **kwargs):
        super().__init__(signals= [EndOfData, EndOfDay], publications=[self.sanitize_timeframe(timeframe)], **kwargs)
        if os.path.isdir(folder):
            self.folder = folder
        else:
            raise Exception(f'Folder {folder} does not exist')
        self.stock = symbol
        self.start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d %H:%M")
        self.current_date = self.start_date
        self.ended = False
        self.df = None
        self.index = None
        self.offset = offset
        self.market_start_time = datetime.datetime.strptime(market_start_time, "%H:%M:%S")
        self.market_end_time = datetime.datetime.strptime(market_end_time, "%H:%M:%S")
        self.timeframe = self.sanitize_timeframe(timeframe)
        self.current_day = 0
        self.end_sent = False
        self.df_offset_str = '09h15min'
        

    def resample(self, df):
        logic = {'open'  : 'first',
                 'high'  : 'max',
                 'low'   : 'min',
                 'close' : 'last'}
        if self.timeframe.lower() == '1min':
            return df
        if self.timeframe.endswith('Min'):
            df = df.resample(self.timeframe.lower(), offset=self.df_offset_str).apply(logic).dropna()
        else:
            df = df.resample(self.timeframe, offset=self.df_offset_str).apply(logic).dropna()
        return df
    
    async def next(self, connection=None, **kwargs):
        if not self.ready(connection, **kwargs):
            log(f'{self}: Not ready yet', 'debug')
            return
        if self.ended:
            return
        if self.df is None:
            log('Fetch data', 'debug')
            if self.mode in ['backtest', 'buffered']:
                self.df = cached(self.stock, df=None, timeframe=convert_timeframe_to_quant(self.timeframe))
                if self.df is None:
                    #Parse the data from the file sources into a single dataframe
                    log('Fetch fresh data', 'debug')
                    parsing = True
                    df_array = []
                    while parsing:
                        path = os.path.join(self.folder, 
                                            str(self.current_date.year), 
                                            f'{self.current_date.month:02d}',
                                            f'{self.current_date.day:02d}')
                        current_file = os.path.join(path, f'{self.stock}.csv')
                        #log(f'Checking: {current_file}', 'debug')
                        if os.path.exists(current_file):
                            #Parse and append to an array which we will merge finally
                            #log(f'Reading {current_file}', 'debug')
                            df_source = pd.read_csv(current_file,
                                                    header=0,
                                                dtype={'datetime': str,
                                                    'name': str,
                                                    'open': float,
                                                    'high': float,
                                                    'low': float,
                                                    'close': float},
                                                usecols=[0,1,2,3,4,5])
                            df_source['datetime'] = pd.to_datetime(df_source['datetime'], format='%Y-%m-%d %H:%M:%S')
                            df_source.set_index('datetime', inplace=True)
                            df_source.reindex()
                            df_source = df_source.sort_index()
                            #Until initial datetime is less than market hours, drop rows
                            while True:
                                if (df_source.index[0].hour <= self.market_start_time.hour) and \
                                    (df_source.index[0].minute < self.market_start_time.minute):
                                    df_source.drop(index=[df_source.index[0]], inplace=True)
                                else:
                                    break
                            #This is 1 minute data. Resample, if required
                            df_source = self.resample(df_source)

                            df_array.append(df_source)
                        #Increment the day to advance the date
                        self.current_date += relativedelta(days=1)
                        if self.current_date > datetime.datetime.today():
                            parsing = False #Stopping criteria
                    #Now merge the dataframes into a single one
                    if len(df_array)>0:
                        self.df = pd.concat(df_array, axis=0)
                        self.df = self.df[~self.df.index.duplicated(keep='first')]
                        cached(self.stock, self.df, timeframe=convert_timeframe_to_quant(self.timeframe))
                else:
                    self.df['datetime'] = pd.to_datetime(self.df['datetime'], format='%Y-%m-%d %H:%M:%S')
                    self.df.set_index('datetime', inplace=True)
                    self.df.reindex()
                    self.df = self.df.sort_index()
                #log(self.df.head(10), 'debug')
                #log(self.df.tail(1), 'debug')
                #log(self.df.tail(1).isnull().sum().sum(), 'debug')
                #nan_cols = self.df.tail(1)[self.df.tail(1).columns[self.df.tail(1).isnull().any()]]
                #log(nan_cols, 'debug')
            else:
                log('Mode currently not supported', logtype='error')
                await self.emit(EndOfData(timestamp=self.df.index[-1], timeframe=self.timeframe))
                self.ended = True
        else:
            if self.mode not in ['backtest', 'buffered']:
                log('Mode currently not supported', logtype='error')
                await self.emit(EndOfData(timestamp=self.df.index[-1], timeframe=self.timeframe))
                self.ended = True
        #log(self.df.tail(1), 'debug')
        #log(len(self.df), 'debug')
        if self.index is None:
            self.index = self.offset
            self.df_offset_str = f'{self.df.index[0].hour}h{self.df.index[0].minute}min'
        if self.index < len(self.df):
            df = self.df.iloc[0:self.index+1].copy()
            if len(df)>0:
                self.last_ts = df.index[-1]
                #log(f'{self.name}: Source: {df.tail(1)}', 'debug')
                for node,connection in self.connections:
                    await node.next(connection=connection, data = df.copy(deep=True))
                await self.notify(df)
                if self.current_day != self.last_ts.day:
                    #Send EndOfDay notification only once, not beyond it on the same day. Here, day has changed now
                    self.current_day = self.last_ts.day
                    self.end_sent = False
                if (self.last_ts.hour >= self.market_end_time.hour) and \
                        (self.last_ts.minute >= self.market_end_time.minute) and \
                        (self.end_sent is False):
                    await self.emit(EndOfDay(timestamp=self.last_ts, timeframe=self.timeframe, df=df))
                    self.end_sent = True
                self.consume()
            else:
                log('No data to pass', 'debug')
                self.consume()
                return
            self.index +=1
        else:
            if not self.ended:
                await self.emit(EndOfData(timestamp=self.df.index[-1], timeframe=self.timeframe))
                self.ended = True
            #log('Source Returning', 'debug')
            return
