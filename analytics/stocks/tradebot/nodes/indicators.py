from tradebot.base import FlowGraphNode
import talib as ta
from talib.abstract import *
from lib.logging import log
import pandas as pd

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

    async def next(self, connection=None, **kwargs):
        if not self.ready(connection, **kwargs):
            log(f'{self}: Not ready yet', 'debug')
            return
        df = kwargs.get('data')
        log(f'{self}: {df.tail(0)}', 'debug')
        for indicator in self.indicators:
            df[indicator] = self.indicators[indicator]['method'](df[self.indicators[indicator]['column']], **self.indicators[indicator]['attributes'])
        for node,connection in self.connections:
            await node.next(connection=connection, data = df)
        self.consume()

class Indicator(FlowGraphNode):
    '''
    The output of this node is just the last value (row) of the indicator
    '''
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

    async def next(self, connection=None, **kwargs):
        if not self.ready(connection, **kwargs):
            log(f'{self}: Not ready yet', 'debug')
            return
        df = kwargs.get('data')
        log(f'{self}: {df.tail(0)}', 'debug')
        '''
        Here, we add a multi-level index in order to support multiple indicators in a single node.
        This is like:

        Input: 

        datetime   |        stock_1          |           stock_2           |
         <>        |        1234             |           4567              |
        Output:

                   |        stock_1        |          stock_2      |
        datetime   | close   ind_1   ind_2 | close   ind_1   ind_2 |
         <>        | 1234     x_1     y_1  | 4567     x_2     y_2  |

        We could totally have multiple nodes of a single level index and support only a single 
        indicator per node, but that would involve unnecessary copying of inputs to that many nodes.

        What we can do is, if there is only a single column, we can drop the level, but that would 
        make the design a bit convoluted
        '''
        #df.columns = pd.MultiIndex.from_product([df.columns, ['close']])
        #https://stackoverflow.com/questions/40225683/how-to-simply-add-a-column-level-to-a-pandas-dataframe
        if len(list(df.columns))==1:
            for indicator in self.indicators:
                df[indicator] = df.apply(lambda x: self.indicators[indicator]['method'](x, **self.indicators[indicator]['attributes']))
            for node,connection in self.connections:
                await node.next(connection=connection, data = df.tail(1))
        else:
            for indicator in self.indicators:
                df[indicator] = df.apply(lambda x: self.indicators[indicator]['method'](x, **self.indicators[indicator]['attributes']))
        self.consume()