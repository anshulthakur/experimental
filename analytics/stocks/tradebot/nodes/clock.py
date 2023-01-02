from tradebot.base import FlowGraphNode
from lib.logging import log

class TimeResampler(FlowGraphNode):
    def __init__(self, interval):
        self.interval = interval
        self.reset_ticks = self.interval_to_ticks(self.interval)
        self.elapsed_ticks = 0

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

    async def next(self, tick):
        if self.elapsed_ticks==0:
            for node in self.connections:
                await node.next(tick)
        self.elapsed_ticks += 1
        if self.elapsed_ticks == self.reset_ticks:
            self.elapsed_ticks = 0

class Resampler(TimeResampler):
    pass