
from .base import BaseClass

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
    def __init__(self, name, level, scrip=None, key='close', condition='<=', recurring=False, timeframe='1m', filters = []):
        self.name = name
        self.level_key = None
        self.scrip = None
        self.filters = filters if filters is not None else []
        try:
            self.level = float(level)
        except:
            self.level = None
            if level is None:
                raise Exception('Level field cannot be None')
            if scrip is None:
                raise Exception('Scrip field cannot be None')
            self.scrip = scrip
        self.key = key
        self.condition = condition
        self.recurring = recurring
        self.timeframe = self.sanitize_timeframe(timeframe)
        self.active = True
        self.subscriber = None
        self.df = None #Will contain the dataframe row of trigger
    
    def trigger(self, df):
        stocks = []
        try:
            stocks = list(df.columns.levels[0])
            columns = list(df.columns.levels[1])
            output = {s: None for s in stocks}
        except:
            columns = list(df.columns)
        if len(stocks)>0:
            #Multi-level dataframe
            if self.scrip not in stocks:
                #Don't process if stock not in dataframe
                return False
            for filter in self.filters:
                if filter.filter(df[self.scrip]) is False: #Filter criteria not matched yet
                    return False
            if self.level is not None:
                if eval(f'{df[self.scrip][self.key][-1]}{self.condition}{self.level}') is True:
                    #self.df = df.loc[df.index[-1]]
                    self.df = df[self.scrip].tail(1)
                    self.active = False if self.recurring is False else True
                    return True
            elif self.level_key in columns:
                if eval(f'{df[self.scrip][self.key][-1]}{self.condition}{df[self.scrip][self.level_key][-1]}') is True:
                    #self.df = df.loc[df.index[-1]]
                    self.df = df[self.scrip].tail(1)
                    self.active = False if self.recurring is False else True
                    return True
        else:
            #Single-level dataframe
            if self.key in list(df.columns):
                for filter in self.filters:
                    if filter.filter(df) is False: #Filter criteria not matched yet
                        return False 
                if self.level is not None:
                    if eval(f'{df[self.key][-1]}{self.condition}{self.level}') is True:
                        #self.df = df.loc[df.index[-1]]
                        self.df = df.tail(1)
                        self.active = False if self.recurring is False else True
                        return True
                elif self.level_key in list(df.columns):
                    if eval(f'{df[self.key][-1]}{self.condition}{df[self.level_key][-1]}') is True:
                        #self.df = df.loc[df.index[-1]]
                        self.df = df.tail(1)
                        self.active = False if self.recurring is False else True
                        return True
        return False
    
    def __str__(self) -> str:
        return f'{self.name} ({self.timeframe})'
    