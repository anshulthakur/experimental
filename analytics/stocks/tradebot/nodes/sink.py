from tradebot.base import FlowGraphNode
from lib.logging import log

from tradebot.base.signals import *
import pandas as pd 
import json
import numpy as np

#https://sebhastian.com/python-object-of-type-int64-is-not-json-serializable/
class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)

class SinkNode(FlowGraphNode):
    def __init__(self, print_logs=True, **kwargs):
        super().__init__(**kwargs)
        self.print_logs = print_logs

    def put(self, value):
        pass

    async def handle_signal(self, signal):
        if signal.name() == EndOfData.name():
            log("Received end of data", 'debug')
            pass
        else:
            log(f"Unknown signal {signal.name()}")
        return

class Sink(SinkNode):
    def __init__(self, **kwargs):
        super().__init__(strict = False, **kwargs)
        self.multi_input = True
    
    async def next(self, connection=None, **kwargs):
        if not self.ready(connection, **kwargs):
            log(f'{self}: Not ready yet', 'debug')
            return
        #log(f'{self}:', 'debug')
        metadata = kwargs.get('metadata', None)
        for conn in self.inputs:
            df = self.inputs[conn]
            if type(df).__name__ == 'DataFrame':
                if self.print_logs:
                    log(f'{conn}:(Metadata){metadata}', 'debug')
                    log(f'{df.tail(1)}', 'debug')
            elif type(df).__name__ == 'dict':
                if self.print_logs:
                    log(f'{conn}:(Metadata){metadata}', 'debug')
                    log(json.dumps(df, indent=2, cls=NpEncoder), 'debug')
            elif type(df).__name__ == 'list':
                if self.print_logs:
                    log(f'{conn}:(Metadata){metadata}', 'debug')
                    for l in df:
                        log(l, 'debug')
            else:
                if self.print_logs:
                    log(f'{conn}:(Metadata){metadata}', 'debug')
                    log(f'{df}', 'debug')
        self.consume()
        return
    
    async def handle_signal(self, signal):
        if signal.name() == EndOfData.name():
            log("Received end of data", 'debug')
            pass
        elif signal.name() == Shutdown.name():
            log("Received shutdown", 'debug')
            pass
        else:
            log(f"Unknown signal {signal.name()}")
        return


class DataFrameSink(SinkNode):
    def __init__(self, **kwargs):
        #Will save dataframe to a file
        super().__init__(**kwargs)
        self.multi_input = True
        self.wait_for_all = False
    
    async def next(self, connection=None, **kwargs):
        if not self.ready(connection, **kwargs):
            log(f'{self}: Not ready yet', 'debug')
            return
        #log(f'{self}:', 'debug')
        metadata = kwargs.get('metadata', None)
        for conn in self.inputs:
            df = self.inputs[conn]
            if self.print_logs:
                log(f'{conn}:(Metadata){metadata}', 'debug')
                log(f'{df.tail(1)}', 'debug')
            pass
        self.consume()
        return
    
    async def handle_signal(self, signal):
        if signal.name() == EndOfData.name():
            log("Received end of data", 'debug')
            pass
        elif signal.name() == Shutdown.name():
            log("Received shutdown", 'debug')
            pass
        else:
            log(f"Unknown signal {signal.name()}")
        return


class FileSink(SinkNode):
    def __init__(self, filename, **kwargs):
        #Will save dataframe to a file
        self.filename = filename
        self.multi_input = True
        super().__init__(**kwargs)
    
    async def next(self, connection=None, **kwargs):
        if not self.ready(connection, **kwargs):
            log(f'{self}: Not ready yet', 'debug')
            return
        metadata = kwargs.get('metadata', None)
        log(f'{self}:', 'debug')
        for conn in self.inputs:
            df = self.inputs[conn]
            log(f'{conn}:(Metadata){metadata}', 'debug')
            log(f'{df.tail(1)}', 'debug')
        self.consume()
        return


class DataFrameAggregator(SinkNode):
    def __init__(self, filename=None, **kwargs):
        #Will save dataframe to a file
        self.filename = filename
        self.multi_input = False
        self.df = pd.DataFrame()

        super().__init__(**kwargs)
    
    async def next(self, connection=None, **kwargs):
        if not self.ready(connection, **kwargs):
            log(f'{self}: Not ready yet', 'debug')
            return
        #log(f'{self}:', 'debug')
        for conn in self.inputs:
            if self.df.empty:
                self.df = self.inputs[conn]
            else: 
                self.df = pd.concat([self.df, self.inputs[conn]], join='outer', sort=True) 
                self.df.drop_duplicates(inplace=True)
            #log(f'{self}', 'debug')
            #log(f'{self.df.tail(1)}', 'debug')
        self.consume()
        return
    
    async def handle_signal(self, signal):
        if signal.name() in [Resistance.name(),Support.name()]:
            log(f"[{signal.timestamp}] {signal.name()} : {signal.value} ({signal.index})", 'debug')
        elif signal.name() == EndOfData.name():
            log("Received end of data", 'debug')
            pass
        else:
            log(f"Unknown signal {signal.name()}", 'debug')
        #log('Sink Returning', 'debug')
        return

class Recorder(FlowGraphNode):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    async def handle_signal(self, signal):
        if signal.name() == EndOfData.name():
            log("Received end of data", 'debug')
            pass
        else:
            log(f"Unknown signal {signal.name()}")
        return
