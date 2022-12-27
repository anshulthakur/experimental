from ..base import FlowGraphNode
from ...lib.logging import log

class SinkNode(FlowGraphNode):
    def __init__(self):
        super().__init__()

    def put(self, value):
        pass

class FileSink(SinkNode):
    def __init__(self, filename):
        #Will save dataframe to a file
        self.filename = filename
        super().__init__()