from tradebot.base import FlowGraphNode, BaseFilter
from lib.logging import log
import pandas as pd

class BaseScreen(FlowGraphNode):
    epsilon = 0.00001
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.multi_input = True
        self.wait_for_all = False
        self.column_names = []
        self.filters = self.create_filters()
        
    def create_filters(self):
        return []

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
        df = kwargs.pop('data')

        stocks = []
        #log(f"{self.name}", 'debug')
        #log(f"{df.tail(1)}", 'debug')
        try:
            stocks = list(df.columns.levels[0])
            columns = list(df.columns.levels[1])
            output = {s: None for s in stocks}
        except:
            columns = list(df.columns)

        if len(stocks)>0:
            for stock in stocks:
                #log(f'{stock}', 'debug')
                for column in self.column_names:
                    if column not in columns:
                        raise Exception(f"{column} not present in input dataframe for {stock}.")
                if pd.isna(df[stock].iloc[-1]).sum().sum() > 0: #One of the columns is NaN. Filters cannot apply
                    #log(f"{stock} has {pd.isna(df[stock].iloc[-1]).sum().sum()} NaN elements")
                    output.pop(stock, None)
                    continue
                for filter in self.filters:
                    #log(f'{filter}', 'debug')
                    if filter.filter(df[stock]) is False: #One filter failed, move to next stock
                        output.pop(stock, None)
                        #log(f'Filter criteria Failed', 'debug')
                        break
        else:
            for column in self.column_names:
                if column not in columns:
                    raise Exception(f"{column} not present in input dataframe.")
                elif pd.isna(df.iloc[-1][column]): #Column is NaN. Filters cannot apply
                    log(f"{column} is NaN.")
                    self.consume()
                    return
            for filter in self.filters:
                if filter.filter(df) is False: #One filter failed, return
                    self.consume()
                    return
        if len(output)>0:
            for stock in output:
                output[stock] = df[stock].iloc[-1].to_dict()
            for node,connection in self.connections:
                await node.next(connection=connection, data = output, **kwargs)
        self.consume()

class CustomScreen(BaseScreen):
    def __init__(self, filters=[], **kwargs):
        super().__init__(**kwargs)
        self.column_names = []
        for filter in filters:
            self.filters.append(filter)
            for column_name in filter.column_names:
                if column_name not in self.column_names:
                    self.column_names.append(column_name)

class EMA_Filter(BaseFilter):
    def __init__(self, value, greater=True, what='close'):
        self.column_names = ['EMA'+str(value)]
        if greater==True:
            self.filter = lambda x: True if x.iloc[-1][what]>=x.iloc[-1]['EMA'+str(value)] else False
        else:
            self.filter = lambda x: True if x.iloc[-1][what]<=x.iloc[-1]['EMA'+str(value)] else False

class RSI_Filter(BaseFilter):
    def __init__(self, value=65, greater=True, **kwargs):
        self.column_names = ['RSI']
        if greater==True:
            #self.filter = lambda x: True if ((not pd.isna(x['RSI'][-1])) and x['RSI'][-1]>=value) else False
            self.filter = lambda x: True if ((not pd.isna(x.iloc[-1]['RSI'])) and x.iloc[-1]['RSI']>=value) else False
        else:
            self.filter = lambda x: True if ((not pd.isna(x.iloc[-1]['RSI'])) and x.iloc[-1]['RSI']<=value) else False

class Eval_Filter(BaseFilter):
    def __init__(self, condition, column_names=['open', 'high', 'low', 'close'], **kwargs):
        super().__init__(column_names, **kwargs)
        self.condition = condition
        self.filter = lambda x: True if eval(f'{self.condition}') else False

class Price_Filter(BaseFilter):
    def create_filter(self):
        def lt(x):
            if self.level is not None:
                if self.level >= 0:
                    return True if x.iloc[-1][self.key] < self.level else False 
                else:
                    return True if x.iloc[-1][self.key] < x[self.key][int(self.level)] else False 
            else:
                return True if x.iloc[-1][self.key] < x.iloc[-1][self.level_key] else False 

        def leq(x):
            if self.level is not None:
                if self.level >= 0:
                    return True if x.iloc[-1][self.key] <= self.level else False 
                else:
                    return True if x.iloc[-1][self.key] <= x[self.key][int(self.level)] else False 
            else:
                return True if x.iloc[-1][self.key] <= x.iloc[-1][self.level_key] else False 
        def gt(x):
            if self.level is not None:
                if self.level >= 0:
                    return True if x.iloc[-1][self.key] > self.level else False 
                else:
                    return True if x.iloc[-1][self.key] > x[self.key][int(self.level)] else False 
            else:
                return True if x.iloc[-1][self.key] >= x.iloc[-1][self.level_key] else False 

        def geq(x):
            if self.level is not None:
                if self.level >= 0:
                    return True if x.iloc[-1][self.key] >= self.level else False 
                else:
                    return True if x.iloc[-1][self.key] >= x[self.key][int(self.level)] else False 
            else:
                return True if x.iloc[-1][self.key] >= x.iloc[-1][self.level_key] else False 

        def eq(x):
            if self.level is not None:
                if self.level >= 0:
                    return True if x.iloc[-1][self.key] == self.level else False 
                else:
                    return True if x.iloc[-1][self.key] == x[self.key][int(self.level)] else False 
            else:
                return True if x.iloc[-1][self.key] == x.iloc[-1][self.level_key] else False 

        def proximity(x):
            if self.level is not None:
                if self.level >= 0:
                    if (x.iloc[-1][self.key] >= self.level) and (x.iloc[-1][self.key]-self.level)/self.level <= self.margin:
                        return True
                    elif (self.level > x.iloc[-1][self.key]) and (self.level -x.iloc[-1][self.key])/self.level <= self.margin:
                        return True
                else:
                    if (x.iloc[-1][self.key] >= x.iloc[int(self.level)][self.key]) and (x.iloc[-1][self.key]-x.iloc[int(self.level)][self.key])/x.iloc[int(self.level)][self.key] <= self.margin:
                        return True
                    elif (x.iloc[int(self.level)][self.key] > x.iloc[-1][self.key]) and (x.iloc[int(self.level)][self.key] -x.iloc[-1][self.key])/x.iloc[int(self.level)][self.key] <= self.margin:
                        return True
            else:
                if (x.iloc[-1][self.key] >= x.iloc[-1][self.level_key]) and (x.iloc[-1][self.key] - x.iloc[-1][self.level_key])/x.iloc[-1][self.level_key] <= self.margin:
                    return True
                elif (x.iloc[-1][self.key] < x.iloc[-1][self.level_key]) and (x.iloc[-1][self.level_key] - x[self.key][-1])/x.iloc[-1][self.level_key] <= self.margin:
                    return True
            return False
        
        filter_map = {'<=': leq,
                      '>=': geq,
                      '<': lt,
                      '>': gt,
                      '=': eq,
                      'near': proximity}
        return filter_map.get(self.condition.lower().strip(), None)
    
    def __init__(self, level, key='close', condition='<=', **kwargs):
        self.column_names = ['close']
        self.level_key = None
        self.margin = float(kwargs.get('margin', 0.0001))
        try:
            self.level = float(level)
        except:
            self.level = None
            if level is None:
                raise Exception('Level field cannot be None')
            self.level_key = level
        self.key = key
        self.condition = condition
        self.filter = self.create_filter()


class DivergenceFilter(BaseFilter):
    '''
    Divergence filter filters the stock if it is exhibiting divergence (as per the divergence field)

    @param indicator The column name of the indicator for which indicator is being looked at
    @param positive Flag indicating whether to filter positive divergence or negative
    '''
    def __init__(self, indicator='RSI', positive=True):
        self.indicator = indicator+'_divergence'
        if positive==True:
            self.filter = lambda x: True if ((not pd.isna(x.iloc[-1][self.indicator])) and x.iloc[-1][self.indicator]>0) else False
        else:
            self.filter = lambda x: True if ((not pd.isna(x.iloc[-1][self.indicator])) and x.iloc[-1][self.indicator]<0) else False

class Crossover_Screen(BaseScreen):
    def __init__(self,  crosses, what='close', direction='None', **kwargs):
        super().__init__(**kwargs)
        self.what = what
        if type(crosses) == str:
            self.column_names = [crosses, what]
            self.crosses = crosses
        elif type(crosses) == float or type(crosses) == int:
            self.column_names = [what]
            self.crosses = float(crosses) if crosses != 0 else self.epsilon
        self.direction = direction.lower()

        self.filters = self.create_filters()

        if kwargs.get('filters', None) is not None:
            for filter in kwargs.get('filters'):
                log(f'Add custom filter to {self}', 'debug')
                self.filters.append(filter)
                for column_name in filter.column_names:
                    if column_name not in self.column_names:
                        self.column_names.append(column_name)

    def create_filters(self):
        def crossover(x):
            #log(x, 'debug')
            if self.direction in ['none', 'up']:
                if type(self.crosses) == float:
                    if x.iloc[-1][self.what]>=self.crosses and x.iloc[-2][self.what]<self.crosses:
                        return True
                else:
                    if x.iloc[-1][self.what]>=x.iloc[-1][self.crosses] and x.iloc[-2][self.what]<x.iloc[-2][self.crosses]:
                        return True
            elif self.direction in ['none', 'down']:
                if type(self.crosses) == float:
                    if x.iloc[-1][self.what]<self.crosses and x.iloc[-2][self.what]>=self.crosses:
                        return True
                else:
                    if x.iloc[-1][self.what]<x.iloc[-1][self.crosses] and x.iloc[-2][self.what]>=x.iloc[-2][self.crosses]:
                        return True
            return False
        filters = []
        filters.append(BaseFilter(filter=crossover))
        return filters

class Proximity_Screen(BaseScreen):
    def __init__(self,  near, by, what='close', direction='None', **kwargs):
        super().__init__(**kwargs)
        self.what = what
        if type(near) == str:
            self.column_names = [near, what]
            self.near = near
        elif type(near) == float or type(near) == int:
            self.column_names = [what]
            self.near = float(near) if near != 0 else self.epsilon
        self.margin=by #Fraction
        self.direction = direction.lower()

        self.filters = self.create_filters()

        if kwargs.get('filters', None) is not None:
            for filter in kwargs.get('filters'):
                log(f'Add custom filter to {self}', 'debug')
                self.filters.append(filter)
                for column_name in filter.column_names:
                    if column_name not in self.column_names:
                        self.column_names.append(column_name)

    def create_filters(self):
        def proximity(x):
            #log(x, 'debug')
            if self.direction in ['none', 'up']:
                if type(self.near) == float:
                    if (x.iloc[-1][self.what] > self.near) and (x.iloc[-1][self.what]-self.near)/self.near <= self.margin:
                        return True
                else:
                    if (x.iloc[-1][self.what] > x.iloc[-1][self.near]) and (x.iloc[-1][self.what]-x.iloc[-1][self.near])/x.iloc[-1][self.near] <= self.margin:
                        return True
            elif self.direction in ['none', 'down']:
                if type(self.near) == float:
                    if (self.near > x.iloc[-1][self.what]) and (self.near - x.iloc[-1][self.what])/self.near <= self.margin:
                        return True
                else:
                    if (x.iloc[-1][self.near] > x.iloc[-1][self.what]) and (x.iloc[-1][self.near] - x.iloc[-1][self.what])/x.iloc[-1][self.near] <= self.margin:
                        return True
            return False
        filters = []
        filters.append(BaseFilter(filter=proximity))
        return filters


#These are implementation examples of what we have done here
class EMA_RSI_Screen(BaseScreen):
    def create_filters(self):
        self.column_names = ['close', 'EMA20', 'EMA200', 'RSI']
        filters = []
        filters.append(EMA_Filter(value=20))
        filters.append(EMA_Filter(value=200))
        filters.append(RSI_Filter(value=65))
        return filters

class DivergenceScreen(BaseScreen):
    def __init__(self, indicator='RSI', **kwargs):
        super().__init__(**kwargs)
        self.indicator = indicator
        self.column_names = ['close', self.indicator+'_divergence']
        self.filters = self.create_filters()

    def create_filters(self):
        filters = []
        filters.append(DivergenceFilter(indicator=self.indicator))
        return filters