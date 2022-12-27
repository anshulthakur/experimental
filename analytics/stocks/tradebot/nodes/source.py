from ..base import FlowGraphNode
from ...lib.logging import log

class SourceNode(FlowGraphNode):
    def __init__(self):
        super().__init__()

    def next(self):
        pass

class TradingViewSource(SourceNode):
    def __init__(self, symbol, exchange, timeframe):
        super().__init__()

class DbSource(SourceNode):
    def __init__(self, symbol, exchange, timeframe):
        super().__init__()

class CsvSource(SourceNode):
    def __init__(self, filename):
        self.filename = filename
        super().__init__()

class YahooSource(SourceNode):
    def __init__(self, symbol, timeframe):
        super().__init__()