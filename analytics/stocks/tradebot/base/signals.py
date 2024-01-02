
from .base import BaseClass, BaseFilter
from lib.logging import log

class BaseSignal(BaseClass):
    @classmethod
    def name(cls):
        raise Exception('Cannot invoke BaseSignal')

    def __init__(self, **kwargs):
        pass

    def __str__(self):
        return self.name

class EndOfData(BaseSignal):
    @classmethod
    def name(cls):
        return 'EndOfData'
    def __init__(self, timestamp, **kwargs):
        self.timestamp = timestamp
        self.timeframe = kwargs.get('timeframe', None)
        super().__init__(**kwargs)
    
    def __str__(self):
        return f"{self.name}[{self.timestamp.to_pydatetime()}] End of data"

class EndOfDay(BaseSignal):
    @classmethod
    def name(cls):
        return 'EndOfDay'
    def __init__(self, timestamp, df, **kwargs):
        self.timestamp = timestamp
        self.timeframe = kwargs.get('timeframe', None)
        self.df = df.tail(1)
        super().__init__(**kwargs)
    
    def __str__(self):
        return f"{self.name}[{self.timestamp.to_pydatetime()}] End of data"

class Shutdown(BaseSignal):
    @classmethod
    def name(cls):
        return 'Shutdown'
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
    
    def __str__(self):
        return f"{self.name} Shutting down"

class Resistance(BaseSignal):
    @classmethod
    def name(cls):
        return 'Resistance'

    def __init__(self, index, value, timestamp, **kwargs):
        super().__init__(**kwargs)
        self.index = index
        self.value = value
        self.timestamp = timestamp
    
    def __str__(self):
        return f"{self.name}[{self.timestamp.to_pydatetime()}] Resistance at {self.value} ({self.index})"

class Support(BaseSignal):
    @classmethod
    def name(cls):
        return 'Support'

    def __init__(self, index, value, timestamp, **kwargs):
        super().__init__(**kwargs)
        self.index = index
        self.value = value
        self.timestamp = timestamp
        self.timeframe = kwargs.get('timeframe', None)

    def __str__(self):
        return f"{self.name}[{self.timestamp.to_pydatetime()}] Support at {self.value} ({self.index})"


class Alert(BaseClass):
    def create_filter(self):
        def lt(x):
            if self.level is not None:
                return True if x.iloc[-1][self.key] < self.level else False 
            else:
                return True if x.iloc[-1][self.key] < x.iloc[-1][self.level_key] else False 
            
        def leq(x):
            if self.level is not None:
                return True if x.iloc[-1][self.key] <= self.level else False 
            else:
                return True if x.iloc[-1][self.key] <= x.iloc[-1][self.level_key] else False 
        def gt(x):
            if self.level is not None:
                return True if x.iloc[-1][self.key] > self.level else False 
            else:
                return True if x.iloc[-1][self.key] >= x.iloc[-1][self.level_key] else False 
            
        def geq(x):
            if self.level is not None:
                return True if x.iloc[-1][self.key] >= self.level else False 
            else:
                return True if x.iloc[-1][self.key] >= x.iloc[-1][self.level_key] else False 
        
        def eq(x):
            if self.level is not None:
                return True if x.iloc[-1][self.key] == self.level else False 
            else:
                return True if x.iloc[-1][self.key] == x.iloc[-1][self.level_key] else False 

        def proximity(x):
            if self.level is not None:
                if (x.iloc[-1][self.key] >= self.level) and (x.iloc[-1][self.key]-self.level)/self.level <= self.margin:
                    return True
                elif (self.level > x.iloc[-1][self.key]) and (self.level -x.iloc[-1][self.key])/self.level <= self.margin:
                    return True
            else:
                if (x.iloc[-1][self.key] >= x.iloc[-1][self.level_key]) and (x.iloc[-1][self.key] - x.iloc[-1][self.level_key])/x.iloc[-1][self.level_key] <= self.margin:
                    return True
                elif (x.iloc[-1][self.key] < x.iloc[-1][self.level_key]) and (x.iloc[-1][self.level_key] - x.iloc[-1][self.key])/x.iloc[-1][self.level_key] <= self.margin:
                    return True
            return False
        
        filter_map = {'<=': leq,
                      '>=': geq,
                      '<': lt,
                      '>': gt,
                      '=': eq,
                      'near': proximity}
        return filter_map.get(self.condition.lower().strip(), None)


    def __init__(self, name, level, scrip=None, key='close', condition='<=', recurring=False, timeframe='1m', filters = [], **kwargs):
        self.name = name
        self.level_key = None
        self.scrip = scrip
        self.margin = float(kwargs.get('margin', 0.0001))
        try:
            self.level = float(level)
        except:
            self.level = None
            if level is None:
                raise Exception('Level field cannot be None')
            if scrip is None:
                raise Exception('Scrip field cannot be None')
            self.level_key = level
        self.key = key
        self.condition = condition
        self.recurring = recurring
        self.timeframe = self.sanitize_timeframe(timeframe)
        self.active = True
        self.subscriber = None
        self.df = None #Will contain the dataframe row of trigger
        self.main_filter = None
        try:
            self.main_filter = self.create_filter()
        except:
            raise Exception('Invalid filter condition passed')
        self.filters = filters if filters is not None else []


    def add_filters(self, filters):
        for filter in filters:
            self.filters.append(filter)
    
    def trigger(self, df):
        stocks = []
        try:
            stocks = list(df.columns.levels[0])
            columns = list(df.columns.levels[1])
        except:
            columns = list(df.columns)
        if len(stocks)>0:
            #Multi-level dataframe
            if self.scrip not in stocks:
                #Don't process if stock not in dataframe
                #log(f'Scrip {self.scrip} not present in {stocks}')
                return False
            for filter in self.filters:
                #log(f'Test {filter}')
                if filter.filter(df[self.scrip]) is False: #Filter criteria not matched yet
                    #log(f'Filter: {self.scrip} not met', 'debug')
                    return False
            if self.main_filter(df[self.scrip]) == True:
                #log(f'Main filter passed', 'debug')
                self.df = df.tail(1)
                self.active = False if self.recurring is False else True
                log(f'Passed: [{self.df.index[-1].to_pydatetime()}] {self}')
                return True
            #log('Return false')
            '''
            if self.level is not None:
                if eval(f'{df[self.scrip].iloc[-1][self.key]}{self.condition}{self.level}') is True:
                    #self.df = df.loc[df.index[-1]]
                    self.df = df[self.scrip].tail(1)
                    self.active = False if self.recurring is False else True
                    return True
            elif self.level_key in columns:
                if eval(f'{df[self.scrip].iloc[-1][self.key]}{self.condition}{df[self.scrip].iloc[-1][self.level_key]}') is True:
                    #self.df = df.loc[df.index[-1]]
                    self.df = df[self.scrip].tail(1)
                    self.active = False if self.recurring is False else True
                    return True
            '''
        else:
            #Single-level dataframe
            if self.key in list(df.columns):
                for filter in self.filters:
                    if filter.filter(df) is False: #Filter criteria not matched yet
                        return False 
                if self.main_filter(df) == True:
                    self.df = df.tail(1)
                    self.active = False if self.recurring is False else True
                    return True
                '''
                if self.level is not None:
                    if eval(f'{df.iloc[-1][self.key]}{self.condition}{self.level}') is True:
                        #self.df = df.loc[df.index[-1]]
                        self.df = df.tail(1)
                        self.active = False if self.recurring is False else True
                        return True
                elif self.level_key in list(df.columns):
                    if eval(f'{df.iloc[-1][self.key]}{self.condition}{df.iloc[-1][self.level_key]}') is True:
                        #self.df = df.loc[df.index[-1]]
                        self.df = df.tail(1)
                        self.active = False if self.recurring is False else True
                        return True
                '''
        return False
    
    def __str__(self) -> str:
        return f'({self.timeframe}){self.name}: {self.scrip if self.scrip is not None else ""} close{self.condition}{self.level if self.level is not None else self.level_key}'
    