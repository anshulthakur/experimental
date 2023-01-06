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
    
    async def next(self, connection=None, **kwargs):
        if not self.ready(connection, **kwargs):
            log(f'{self}: Not ready yet', 'debug')
            return
        log(f'{self}:', 'debug')
        for conn in self.inputs:
            df = self.inputs[conn]
            log(f'{conn}', 'debug')
            log(f'{df.tail(1)}', 'debug')
        self.consume()
        return