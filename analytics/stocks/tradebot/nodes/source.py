from tradebot.base import FlowGraphNode
from lib.logging import log

from lib.nse import NseIndia
from tradebot.base.signals import EndOfData
import pandas as pd

from lib.indices import get_index_members, load_index_members

from lib.tradingview import convert_timeframe_to_quant, get_tvfeed_instance, Interval

class SourceNode(FlowGraphNode):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def next(self, connection, **kwargs):
        pass

class TradingViewSource(SourceNode):
    def __init__(self, symbol=None, exchange='NSE', timeframe='1d', **kwargs):
        super().__init__(signals= [EndOfData], **kwargs)
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
                self.consume()

class DbSource(SourceNode):
    def __init__(self, symbol, exchange, timeframe, **kwargs):
        super().__init__(**kwargs)

class CsvSource(SourceNode):
    def __init__(self, filename, **kwargs):
        self.filename = filename
        super().__init__(**kwargs)

class YahooSource(SourceNode):
    def __init__(self, symbol, timeframe, **kwargs):
        super().__init__(**kwargs)

class NseSource(SourceNode):
    def sanitize_timeframe(self, timeframe):
        if isinstance(timeframe, str) and timeframe[-1] not in ['m', 'M', 'h', 'H', 'W', 'D', 'd', 'w']:
            if timeframe.endswith(tuple(['min', 'Min'])):
                if timeframe[0:-3].isnumeric():
                    if int(timeframe[0:-3]) < 60:
                        return f'{timeframe[0:-3]}Min'
                    if int(timeframe[0:-3]) < 60*24:
                        return f'{timeframe[0:-3]//60}H'
                    if int(timeframe[0:-3]) < 60*24*7:
                        return f'{timeframe[0:-3]//(60*7)}W'
                    if int(timeframe[0:-3]) < 60*24*30:
                        return f'{timeframe[0:-3]//(60*30)}M'
                return timeframe
            log(f'Timeframe "{timeframe[-1]}" cannot be interpreted')
        elif not isinstance(timeframe, str):
            if isinstance(timeframe, int):
                if timeframe < 60:
                    return f'{timeframe}Min'
                if timeframe < 60*24:
                    return f'{timeframe//60}H'
                if timeframe < 60*24*7:
                    return f'{timeframe//(60*7)}W'
                if timeframe < 60*24*30:
                    return f'{timeframe//(60*30)}M'
            else:
                log(f'Timeframe "{timeframe[-1]}" must be a string')
        else:
            if timeframe[0:-1].isnumeric():
                if timeframe[-1] == 'm':
                    if int(timeframe[0:-1]) < 60:
                        return f'{timeframe[0:-1]}Min'
                    if int(timeframe[0:-1]) < 60*24:
                        return f'{timeframe[0:-1]//60}H'
                    if int(timeframe[0:-1]) < 60*24*7:
                        return f'{timeframe[0:-1]//(60*7)}W'
                    if int(timeframe[0:-1]) < 60*24*30:
                        return f'{timeframe[0:-1]//(60*30)}M'
                if timeframe[-1] in ['h', 'H']:
                    if int(timeframe[0:-1]) < 24:
                        return f'{timeframe[0:-1]}H'
                    if int(timeframe[0:-1]) < 24*7:
                        return f'{timeframe[0:-1]//24}D'
                    if int(timeframe[0:-1]) < 24*30:
                        return f'{timeframe[0:-1]//(24*7)}W'
                    if int(timeframe[0:-1]) >= 24*30:
                        return f'{timeframe[0:-1]//(24*30)}M'

    def __init__(self, symbol, timeframe, is_index=True, **kwargs):
        super().__init__(signals= [EndOfData], **kwargs)
        self.timeframe = self.sanitize_timeframe(timeframe)
        self.symbol = symbol

        self.source = NseIndia(timeout=10, legacy=True)
        self.df = None
        self.last_ts = None #Last index served
        self.index = None
        self.ended = False
        #super().__init__(**kwargs)

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
                self.consume()

class NseMultiStockSource(SourceNode):
    '''
    Fetch the latest price for TOTAL MARKET INDEX members in a single API call,
    append it to the previous df and pass the df
    '''
    def sanitize_timeframe(self, timeframe):
        if isinstance(timeframe, str) and timeframe[-1] not in ['m', 'M', 'h', 'H', 'W', 'D', 'd', 'w']:
            if timeframe.endswith(tuple(['min', 'Min'])):
                if timeframe[0:-3].isnumeric():
                    if int(timeframe[0:-3]) < 60:
                        return f'{timeframe[0:-3]}Min'
                    if int(timeframe[0:-3]) < 60*24:
                        return f'{timeframe[0:-1]//60}H'
                    if int(timeframe[0:-3]) < 60*24*7:
                        return f'{timeframe[0:-1]//(60*7)}W'
                    if int(timeframe[0:-3]) < 60*24*30:
                        return f'{timeframe[0:-1]//(60*30)}M'
                return timeframe
            log(f'Timeframe "{timeframe[-1]}" cannot be interpreted')
        elif not isinstance(timeframe, str):
            if isinstance(timeframe, int):
                if timeframe < 60:
                    return f'{timeframe}Min'
                if timeframe < 60*24:
                    return f'{timeframe//60}H'
                if timeframe < 60*24*7:
                    return f'{timeframe//(60*7)}W'
                if timeframe < 60*24*30:
                    return f'{timeframe//(60*30)}M'
            else:
                log(f'Timeframe "{timeframe[-1]}" must be a string')
        else:
            if timeframe[0:-1].isnumeric():
                if timeframe[-1] == 'm':
                    if int(timeframe[0:-1]) < 60:
                        return f'{timeframe[0:-1]}Min'
                    if int(timeframe[0:-1]) < 60*24:
                        return f'{timeframe[0:-1]//60}H'
                    if int(timeframe[0:-1]) < 60*24*7:
                        return f'{timeframe[0:-1]//(60*7)}W'
                    if int(timeframe[0:-1]) < 60*24*30:
                        return f'{timeframe[0:-1]//(60*30)}M'
                if timeframe[-1] in ['h', 'H']:
                    if int(timeframe[0:-1]) < 24:
                        return f'{timeframe[0:-1]}H'
                    if int(timeframe[0:-1]) < 24*7:
                        return f'{timeframe[0:-1]//24}D'
                    if int(timeframe[0:-1]) < 24*30:
                        return f'{timeframe[0:-1]//(24*7)}W'
                    if int(timeframe[0:-1]) >= 24*30:
                        return f'{timeframe[0:-1]//(24*30)}M'

    def __init__(self, timeframe, is_index=True, offline=False, **kwargs):
        super().__init__(signals= [EndOfData], **kwargs)
        self.timeframe = self.sanitize_timeframe(timeframe)

        self.source = NseIndia(timeout=10, legacy=False)
        self.df = None
        self.last_ts = None #Last index served
        self.index = None
        self.ended = False
        #super().__init__(**kwargs)

    async def next(self, connection=None, **kwargs):
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
                members = get_index_members(sector='NIFTY TOTAL MARKET')
                self.df = load_index_members(index='NIFTY TOTAL MARKET', 
                                        members=members, 
                                        load_index=False, 
                                        entries=210,
                                        sampling=self.timeframe)
                pass
            else:
                self.df = self.source.getEquityStockIndices(index='NIFTY TOTAL MARKET')
                #log(self.df.head(10), 'debug')
                #self.df['datetime'] = kwargs.get('data')
                #self.df.set_index('datetime', inplace=True)
                #self.df.sort_index(inplace=True)
        else:
            log('Re-fetch data', 'debug')
            df = self.source.getEquityStockIndices(index='NIFTY TOTAL MARKET')
            #df['datetime'] = kwargs.get('data')
            #df.set_index('datetime', inplace=True)
            #df.sort_index(inplace=True)
            #log(df.tail(10), 'debug')
            self.df = pd.concat([self.df, df], join='outer', sort=True)
            self.df.drop_duplicates(inplace=True)
            #self.df = self.df.loc[self.last_ts:].copy()
        #log(self.df.tail(10), 'debug')
        if self.index is None:
            self.index = 0
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
