from tradebot.base import FlowGraphNode
from lib.logging import log
import pandas as pd
from lib.pivots import getHHIndex, getHLIndex, getLHIndex, getLLIndex

class MinMaxDetector(FlowGraphNode):
    def __init__(self, lookaround=1, **kwargs):
        self.order = lookaround #Lookaround width in which peak must be detected. Minimum should be 1 (creates vote in 3)
        super().__init__(**kwargs)
    
    async def next(self, connection=None, **kwargs):
        '''
        Rather than passing the entire history explicitly, we could simply emit a signal whenever a new
        Local Maxima/Minima is detected. Some other node can do the task of cataloguing the values and 
        using them as per their requirement
        '''
        if not self.ready(connection, **kwargs):
            log(f'{self}: Not ready yet', 'debug')
            return
        df = kwargs.get('data')
        #log(f'{self}: {df.tail(1)}', 'debug')
        
        data = {'HH': [],
                'HL': [],
                'LH': [],
                'LL': []}

        if len(list(df.columns))==1:
            #Find peaks and valleys over the given single column data
            for h_idx in getHHIndex(df['close'].values, order=self.order):
                data['HH'].append(h_idx)
            for h_idx in getLHIndex(df['close'].values, order=self.order):
                data['LH'].append(h_idx)
            for l_idx in getHLIndex(df['close'].values, order=self.order):
                data['HL'].append(l_idx)
            for l_idx in getLLIndex(df['close'].values, order=self.order):
                data['LL'].append(l_idx)
            for node,connection in self.connections:
                #log(f"{self}: {data}", 'debug')
                await node.next(connection=connection, data = data)
        else:
            columns = list(df.columns)
            for node,connection in self.connections:
                log(f"{self}: {data}", 'debug')
                await node.next(connection=connection, data = data)
        self.consume()