from tradebot.base import FlowGraphNode
from lib.logging import log

from lib.nse import NseIndia

class SourceNode(FlowGraphNode):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def next(self):
        pass

class TradingViewSource(SourceNode):
    def __init__(self, symbol, exchange, timeframe, **kwargs):
        super().__init__(**kwargs)

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

    def __init__(self, symbol, timeframe, is_index=True, **kwargs):
        self.timeframe = self.sanitize_timeframe(timeframe)
        self.symbol = symbol

        self.source = NseIndia(timeout=10, legacy=True)
        self.df = None
        self.last_ts = None #Last index served
        self.index = None
        super().__init__(**kwargs)

    async def next(self, **kwargs):
        log(f'{self}: {kwargs}', 'debug')
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
                    log(f'{df.tail(1)}', 'debug')
                    for node in self.connections:
                        await node.next(data = df.copy())
                else:
                    log('No data to pass', 'debug')
                    return
                self.index +=1
        else:
            if self.last_ts == None:
                self.last_ts = self.df.index[-1]
                for node in self.connections:
                    await node.next(data = self.df)
