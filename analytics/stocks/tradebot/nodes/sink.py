from tradebot.base import FlowGraphNode
from lib.logging import log
import pandas as pd 

class SinkNode(FlowGraphNode):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def put(self, value):
        pass

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
        log(f'{self}:', 'debug')
        for conn in self.inputs:
            df = self.inputs[conn]
            #log(f'{conn}', 'debug')
            log(f'{df.tail(1)}', 'debug')
        self.consume()
        return


class DataFrameAggregator(SinkNode):
    def __init__(self, filename, **kwargs):
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
            #log(f'{conn}', 'debug')
            #log(f'{self.df.tail(10)}', 'debug')
        self.consume()
        return