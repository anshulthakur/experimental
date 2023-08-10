from tradebot.base import FlowGraphNode
from lib.logging import log

class TimeResampler(FlowGraphNode):
    """Resample (downsample) the clock such that data is passed to the 
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
        self.reset_ticks = self.interval_to_ticks(self.interval)
        self.elapsed_ticks = 0

        super().__init__(**kwargs)

    def interval_to_ticks(self, interval):
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
        tick = kwargs.get('data', None)
        #log(f'{self}: {tick}', 'debug')
        if self.elapsed_ticks==0:
            for node,connection in self.connections:
                await node.next(connection=connection, **kwargs)
            self.consume()
        self.elapsed_ticks += 1
        if self.elapsed_ticks == self.reset_ticks:
            self.elapsed_ticks = 0
        return

class Resampler(TimeResampler):
    pass