
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
    def __init__(self, name, level, key='close', condition='<=', recurring=False, timeframe='1m'):
        self.name = name
        self.level = float(level)
        self.key = key
        self.condition = condition
        self.recurring = recurring
        self.timeframe = self.sanitize_timeframe(timeframe)
        self.active = True
        self.subscriber = None
        self.df = None #Will contain the dataframe row of trigger
    
    def trigger(self, df):
        if self.key in list(df.columns): 
            if eval(f'{df[self.key][-1]}{self.condition}{self.level}') is True:
                #self.df = df.loc[df.index[-1]]
                self.df = df.tail(1)
                self.active = False if self.recurring is False else True
                return True
        return False
    
    def __str__(self) -> str:
        return f'{self.name} ({self.timeframe})'
    