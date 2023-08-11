from lib.logging import log
import datetime 
import time
import asyncio
from .signals import Shutdown

class AsyncScheduler(object):
    def __init__(self, interval, mode='stream'):
        self.mode = mode
        if self.mode not in ['stream', 'buffered', 'backtest']:
            log(f'Unrecognized mode "{self.mode}".', 'error')
            raise Exception(f'Unrecognized mode "{self.mode}".')
        self.interval = self.to_seconds(interval)
        self.flowgraphs = []
        self.running = False
        self.last_runtime = None

    @classmethod
    def to_seconds(cls, timeframe):
        if isinstance(timeframe, str) and timeframe[-1] not in ['s', 'm', 'h', 'H']:
            if timeframe.endswith(tuple(['min', 'Min'])):
                if timeframe[0:-3].isnumeric():
                    return (int(timeframe[0:-3])*60)
                else:
                    log(f'Timeframe "{timeframe}" cannot be interpreted')
                    raise Exception(f'Timeframe "{timeframe}" cannot be interpreted')
        elif not isinstance(timeframe, str):
            if isinstance(timeframe, int):
                return timeframe
            else:
                log(f'Timeframe "{timeframe[-1]}" must be a proper string or integer (seconds)')
        else:
            if timeframe[0:-1].isnumeric():
                if timeframe[-1] == 'm':
                    return int(timeframe[0:-1])*60
                if timeframe[-1] in ['h', 'H']:
                    return int(timeframe[0:-1])*60*60
                if timeframe[-1] == 's':
                    return int(timeframe[0:-1])

    def register(self, flowgraph):
        self.flowgraphs.append(flowgraph)

    def change_mode(self, mode):
        if self.mode == 'backtest' or mode=='backtest':
            raise Exception('Cannot change mode from or to Backtest')
        elif self.mode == 'stream' and mode == 'buffered':
            raise Exception('Cannot move into buffered mode from stream mode')
        elif self.mode == 'buffered' and mode == 'stream':
            self.mode = mode
        
    def stop(self):
        self.running = False
        return
    
    async def run(self):
        self.running = True
        while self.running:
            self.last_runtime = datetime.datetime.now()
            
            for flowgraph in self.flowgraphs:
                await flowgraph.run(data=datetime.datetime.now())
                next_runtime = self.last_runtime + datetime.timedelta(seconds = self.interval) - datetime.datetime.now()
                #log(f'Sleep for {next_runtime.total_seconds()}s', 'debug')
                if self.mode not in ['buffered', 'backtest']: #Wait if we are live, else we should already be having data
                    await asyncio.sleep(next_runtime.total_seconds())
        log('Shutting down')
        for flowgraph in self.flowgraphs:
            await flowgraph.sighandler.emit(Shutdown())

