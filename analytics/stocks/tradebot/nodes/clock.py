from tradebot.base import FlowGraphNode
from lib.logging import log

class TimeResampler(FlowGraphNode):
    """
    Resample (downsample) the clock such that data is passed to the 
    pipeline after resampled delays.

    This node downsamples the rate at which data is passed to subsequent
    nodes in the pipeline. It is useful when the timeframe on which the
    pipeline works is slower than the rate at which the clock scheduler
    is operating. For example, there may be multiple parallel pipelines
    working at different timeframes in a flowgraph. One chain may be working
    on 5min timeframe, while another on 15min. In such cases, the pipeline
    code of the chain running on 15min timeframe must not be invoked before
    the next cycle of 15min elapses. Meanwhile, the 5min timeframe would have 
    been invoked 3 times.
    """
    def __init__(self, interval, **kwargs):
        self.interval = interval
        self.reset_ticks = self.to_ticks(self.interval)
        self.elapsed_ticks = 0

        super().__init__(**kwargs)

    def to_ticks(self, interval):
        if isinstance(interval, str):
            if interval.endswith(('s', 'Sec', 'sec')):
                if interval[-1]=='s':
                    return int(interval[0:-1])
                else:
                    return int(interval[0:-3])
            if interval.endswith(('m', 'Min', 'min')):
                if interval[-1]=='m':
                    return int(interval[0:-1])*60
                else:
                    return int(interval[0:-3])*60
            if interval.endswith(('h', 'H', 'Hr', 'hr')):
                if interval[-1].lower() == 'h':
                    return int(interval[0:-1])*60*60
                else:
                    return int(interval[0:-2])*60*60
            if interval.endswith(('d', 'D')):
                return int(interval[0:-1])*60*60*24
        else:
            return interval

    async def next(self, connection=None, **kwargs):
        if not self.ready(connection, **kwargs):
            log(f'{self}: Not ready yet', 'debug')
            return
        tick = kwargs.pop('data', None)
        #log(f'{self}: {tick}', 'debug')
        self.elapsed_ticks += 1
        if self.elapsed_ticks == self.reset_ticks:
            self.elapsed_ticks = 0
        if self.elapsed_ticks==0:
            for node,connection in self.connections:
                await node.next(connection=connection, **kwargs)
            self.consume()
        return

class Resampler(TimeResampler):
    pass

class DataResampler(FlowGraphNode):
    def __init__(self, interval=1, offset = 15, **kwargs):
        self.interval = self.sanitize_timeframe(timeframe=interval)
        self.reset_ticks = self.to_ticks(interval)
        self.elapsed_ticks = 0
        self.df_offset_str = '09h15min'
        self.offset = int(offset)
        super().__init__(**kwargs)

    def resample(self, df):
        logic = {'open'  : 'first',
                 'high'  : 'max',
                 'low'   : 'min',
                 'close' : 'last'}
        if self.interval.lower() == '1min':
            return df
        if self.interval.endswith('Min'):
            df = df.resample(self.interval.lower(), offset=self.df_offset_str).apply(logic).dropna()
        else:
            df = df.resample(self.interval, offset=self.df_offset_str).apply(logic).dropna()
        return df
    
    def to_ticks(self, interval):
        if isinstance(interval, str):
            if interval.endswith(('s', 'Sec', 'sec')):
                if interval[-1]=='s':
                    return int(interval[0:-1])
                else:
                    return int(interval[0:-3])
            if interval.endswith(('m', 'Min', 'min')):
                if interval[-1]=='m':
                    return int(interval[0:-1])*60
                else:
                    return int(interval[0:-3])*60
            if interval.endswith(('h', 'H', 'Hr', 'hr')):
                if interval[-1].lower() == 'h':
                    return int(interval[0:-1])*60*60
                else:
                    return int(interval[0:-2])*60*60
            if interval.endswith(('d', 'D')):
                return int(interval[0:-1])*60*60*24
        else:
            return interval
        
    async def next(self, connection=None, **kwargs):
        if not self.ready(connection, **kwargs):
            log(f'{self}: Not ready yet', 'debug')
            return
        df = kwargs.pop('data', None)
        metadata = kwargs.pop('metadata', {})
        metadata.update({'timeframe': self.interval})
        '''
        Resampling purely on the basis of samples passed has an issue when we are trying to do multi-timeframe analysis.
        Suppose the candles start at 9:16 (though the market starts at 9:15) and we are sampling with an offset of 9:15.
        Then, an unsampled stream may look like:
        9:16, 
        9:16, 9:17, 
        9:16, 9:17, 9:18, 
        9:16, 9:17, 9:18, 9:19, 
        9:16, 9:17, 9:18, 9:19, 9:20, 
        9:16, 9:17, 9:18, 9:19, 9:20, 9:21 ...

        A 3-sampler would look like:
        .
        ..
        9:15, 9:18
        9:15, 9:18
        9:15, 9:18
        9:15, 9:18, 9:21

        The 9:18 sample should not have been there in the 3rd sample. This is because we expect that a candle data will be made
        available only after it is fully over (unless we ignore the last candle altogether)

        Any downstream node making a decision on the basis of the higher TF candle would 'jump in' fast, set the wrong stop-loss
        and possibly get stopped out at unintentional place.

        At least in a backtest, that is, because time runs faster in a backtest and doesn't take the order time from the system,
        but guesses it as the time we received the candlestick data (end of candle). So, the tradebook would look like:
        Order placed at 9:21, Order closed at 9:19 (stop hit).

        To avoid this, we use the mod of timeframe, while accounting for the offset of market start time (15 min as default). This
        too, however, works only for sub-hour sampling.
        '''
        # if self.df_offset_str is None:
        #     self.df_offset_str = f'{df.index[0].hour}h{df.index[0].minute}min'
        #log(f'{self}: {df.tail(10)}', 'debug')
        #self.elapsed_ticks += 1
        #if self.elapsed_ticks == self.reset_ticks:
        #    self.elapsed_ticks = 0
        #if self.elapsed_ticks==0:
        if (df.index[-1].minute+1-self.offset)%self.reset_ticks==0:#Remove the initial offset of 15 minutes (9:15)
            df = self.resample(df)
            #log(f'{self}: {df.tail(10)}', 'debug')
            for node,connection in self.connections:
                await node.next(connection=connection, data = df.copy(deep=True), metadata=metadata, **kwargs)
            await self.notify(df)
            self.consume()
        return