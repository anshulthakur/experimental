from tradebot.base import FlowGraphNode
import talib as ta
from talib.abstract import *
from lib.logging import log
import pandas as pd
from lib.divergence import detect_divergence, is_diverging

class IndicatorNode(FlowGraphNode):
    '''
    Computes and adds the indicator column to the dataframe containing the OHLC data of a scrip
    '''
    def __init__(self, indicators=[], **kwargs):
        super().__init__(**kwargs)
        self.multi_input = True
        self.wait_for_all = False
        self.indicators = {}
        for indicator in indicators:
            if indicator['tagname'] in self.indicators:
                log(f"Indicator with tagname {indicator['tagname']} already exists in Node.", 'error')
                raise Exception(f"Indicator with tagname {indicator['tagname']} already exists in Node.")
            self.add_indicator(indicator)
        
    
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
        df = kwargs.pop('data')
        log(f'{self}: {df.tail(1)}', 'debug')
        for indicator in self.indicators:
            df[indicator] = self.indicators[indicator]['method'](df[self.indicators[indicator]['column']], **self.indicators[indicator]['attributes'])
        for node,connection in self.connections:
            await node.next(connection=connection, data = df.copy(deep=True), **kwargs)
        self.consume()

class Indicator(FlowGraphNode):
    '''
    This is similar to IndicatorNode, but supports multiple scrips in a single run.
    '''
    def __init__(self, indicators=[], transparent=False, **kwargs):
        super().__init__(**kwargs)
        self.indicators = {}
        self.transparent = transparent
        self.multi_input = True
        self.wait_for_all = False
        for indicator in indicators:
            if indicator['tagname'] in self.indicators:
                log(f"Indicator with tagname {indicator['tagname']} already exists in Node.", 'error')
                raise Exception(f"Indicator with tagname {indicator['tagname']} already exists in Node.")
            self.add_indicator(indicator)
    
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
        df = kwargs.pop('data')
        #log(f'{self}: \n{df.tail(1)}', 'debug')
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
        #https://stackoverflow.com/questions/40225683/how-to-simply-add-a-column-level-to-a-pandas-dataframe
        if len(list(df.columns))==1 or self.transparent:
            for indicator in self.indicators:
                df[indicator] = self.indicators[indicator]['method'](df['close'], **self.indicators[indicator]['attributes'])
            for node,connection in self.connections:
                await node.next(connection=connection, data = df.copy(deep=True), **kwargs)
        else:
            columns = list(df.columns)
            # duplicates = [number for number in columns if columns.count(number) > 1]
            # unique_duplicates = list(set(duplicates))
            # log(f'{unique_duplicates}') # Returns: [2, 3, 5]

            #log(df.tail(1), 'debug')
            '''
            #Variant 1: Simple for small number of columns, but pandas cries performance for large number of columns
            df.columns = pd.MultiIndex.from_product([df.columns, ['close']])
            for column in columns:
              for indicator in self.indicators:
                df[(column,indicator)] = self.indicators[indicator]['method'](df[(column,'close')], **self.indicators[indicator]['attributes'])
            df = df.reindex(
                            pd.MultiIndex.from_product([columns, ["close"]+ [indicator for indicator in self.indicators]]), axis=1
                        )
            '''
            ind_df = {}
            for indicator in self.indicators:
                s_df = df.apply(lambda x: self.indicators[indicator]['method'](x, **self.indicators[indicator]['attributes']), axis=0)
                ind_df[indicator] = s_df
            #Now add a level to all the DFs and concat
            df.columns = pd.MultiIndex.from_product([df.columns, ['close']])
            for indicator in ind_df:
                ind_df[indicator].columns = pd.MultiIndex.from_product([ind_df[indicator].columns, [indicator]])
            ind_df['close'] = df
            n_df = pd.concat([ind_df[d] for d in ind_df], axis='columns', names=[columns, [col for col in ind_df]])

            n_df = n_df.reindex(
                            pd.MultiIndex.from_product([columns, ["close"]+ [indicator for indicator in self.indicators]]), axis=1
                        )
            #log(n_df, 'debug')
            for node,connection in self.connections:
                await node.next(connection=connection, data = n_df.copy(deep=True), **kwargs)
        self.consume()

class Divergence(FlowGraphNode):
    '''
    Calculates if the price is diverging from the indicator and adds a divergence column for each one of
    them.
    By default, it computes the divergence on the closing prices.
    '''
    def __init__(self, indicators=['RSI'], order=1, **kwargs):
        self.indicators = indicators #Indicators to check divergence with
        self.order = order
        super().__init__(**kwargs)

    async def next(self, connection=None, **kwargs):
        if not self.ready(connection, **kwargs):
            log(f'{self}: Not ready yet', 'debug')
            return
        df = kwargs.pop('data')
        for indicator in self.indicators:
            df[f'{indicator}_divergence'] = detect_divergence(df, indicator=indicator, after=None, order=self.order)
        for node,connection in self.connections:
            await node.next(connection=connection, data = df.copy(deep=True), **kwargs)
        self.consume()
