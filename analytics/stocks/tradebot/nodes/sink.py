from tradebot.base import FlowGraphNode
from lib.logging import log

class SinkNode(FlowGraphNode):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def put(self, value):
        pass

class FileSink(SinkNode):
    def __init__(self, filename, **kwargs):
        #Will save dataframe to a file
        self.filename = filename
        super().__init__(**kwargs)
    
    async def next(self, **kwargs):
        log(f'{self}:', 'debug')
        df = kwargs.get('data')
        log(f'{df.tail(1)}', 'debug')
        return