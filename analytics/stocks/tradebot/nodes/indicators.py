from tradebot.base import FlowGraphNode
import talib as ta
from talib.abstract import *
from lib.logging import log

class IndicatorNode(FlowGraphNode):
    def __init__(self, indicators=[], **kwargs):
        self.indicators = {}
        for indicator in indicators:
            if indicator['tagname'] in self.indicators:
                log(f"Indicator with tagname {indicator['tagname']} already exists in Node.", 'error')
                raise Exception(f"Indicator with tagname {indicator['tagname']} already exists in Node.")
            self.add_indicator(indicator)
        super().__init__(**kwargs)
    
    def add_indicator(self, indicator={}):
        if indicator['tagname'] in self.indicators:
            log(f"Indicator with tagname {indicator['tagname']} already exists in Node.", 'error')
            raise Exception(f"Indicator with tagname {indicator['tagname']} already exists in Node.")
        indicator_obj = {'indicator': indicator['type']}
        if indicator['type'] == 'RSI':
            indicator_obj['method'] = ta.RSI
            indicator_obj['column'] = indicator.get('column', 'close')
            indicator_obj['attributes'] = {'timeperiod': indicator.get('length', 14)}
        elif indicator['type'] == 'EMA':
            indicator_obj['method'] = ta.EMA
            indicator_obj['column'] = indicator.get('column', 'close')
            indicator_obj['attributes'] = {'timeperiod': indicator.get('length', 10)}
        self.indicators[indicator['tagname']] = indicator_obj

    async def next(self, **kwargs):
        df = kwargs.get('data')
        log(f'{self}: {df.tail(0)}', 'debug')
        for indicator in self.indicators:
            df[indicator] = self.indicators[indicator]['method'](df[self.indicators[indicator]['column']], **self.indicators[indicator]['attributes'])
        for node in self.connections:
            await node.next(data = df)