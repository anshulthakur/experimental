from tradebot.base import FlowGraphNode
from lib.logging import log
import numpy as np

class Screen(FlowGraphNode):
    def __init__(self,  **kwargs):
        super().__init__(**kwargs)

    async def next(self, connection=None, **kwargs):
        '''
        In a generalized scenario, the screener will be fed multi-level columns with each 
        top-level corresponding to the stock, and the lower level corresponding to the indicator
        '''
        if not self.ready(connection, **kwargs):
            log(f'{self}: Not ready yet', 'debug')
            return
        df = kwargs.get('data')
        
        for node,connection in self.connections:
            await node.next(connection=connection, data = df.copy())
        self.consume()
