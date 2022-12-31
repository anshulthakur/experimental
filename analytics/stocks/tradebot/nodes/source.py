from tradebot.base import FlowGraphNode
from lib.logging import log

class SourceNode(FlowGraphNode):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def next(self):
        pass

class TradingViewSource(SourceNode):
    def __init__(self, symbol, exchange, timeframe, **kwargs):
        super().__init__(**kwargs)

class DbSource(SourceNode):
    def __init__(self, symbol, exchange, timeframe, **kwargs):
        super().__init__(**kwargs)

class CsvSource(SourceNode):
    def __init__(self, filename, **kwargs):
        self.filename = filename
        super().__init__(**kwargs)

class YahooSource(SourceNode):
    def __init__(self, symbol, timeframe, **kwargs):
        super().__init__(**kwargs)

class NseSource(SourceNode):
    def __init__(self, symbol, timeframe, **kwargs):
        super().__init__(**kwargs)

