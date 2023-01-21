from tradebot.base import FlowGraphNode
from lib.logging import log
import pandas as pd

class BaseScreen(FlowGraphNode):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.column_names = []
        self.filters = self.create_filters()
        
    def create_filters(self):
        pass

    async def next(self, connection=None, **kwargs):
        '''
        In a generalized scenario, the screener will be fed multi-level columns with each 
        top-level corresponding to the stock, and the lower level corresponding to the indicator.
        In case of just a single stock data, there won't be multiple levels.

        As a first implementation, the screen works on a single time frame. The names of columns are
        specified as tagnames on which conditions are to be applied.

        In a future version, the screen filters can be specified as a list of strings:
        ['{EMA20} <= {EMA10}',
         '95/100 <= {EMA20}/{EMA10} <= 105/100',
         'RSI > 65']

        Each filter is essentially a function that gets invoked.

        For now, the filters are baked in, and the user must inherit from this class
        and implement their own.

        Output will be a list of stocks which qualify. In case of single stock, 
        the output will be a list with member 'default'.
        '''
        output = ['default']
        if not self.ready(connection, **kwargs):
            log(f'{self}: Not ready yet', 'debug')
            return
        df = kwargs.get('data')
        columns = list(df.columns)
        for column in self.column_names:
            if column not in columns:
                raise Exception(f"{column} not present in input dataframe.")
            elif pd.isna(df[column][-1]): #Column is NaN. Filters cannot apply
                log(f"{column} is NaN.")
                self.consume()
                return
        for filter in self.filters:
            if filter(df) is False: #One filter failed, return
                self.consume()
                return
        
        for node,connection in self.connections:
            await node.next(connection=connection, data = output)
        self.consume()

class EMA_RSI_Screen(BaseScreen):
    def create_filters(self):
        self.column_names = ['EMA20', 'EMA200', 'RSI']
        filters = []
        filters.append(lambda x: x['EMA20'][-1]>=x['EMA200'][-1])
        filters.append(lambda x: True if ((not pd.isna(x['RSI'][-1])) and x['RSI'][-1]>=65) else False)
        return filters
    