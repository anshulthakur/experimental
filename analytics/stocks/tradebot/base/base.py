from lib.logging import log
from datetime import datetime

class BaseFilter(object):
    def __init__(self, filter=None, column_names=[]):
        self.filter = filter
        self.column_names = column_names

    def filter(self):        
        return self.filter
    

class TimeFilter(BaseFilter):
    def __init__(self, value, condition):
        self.level = datetime.strptime(value.strip(), "%Y-%m-%d %H:%M:%S")
        self.condition = condition
        self.filter = self.create_filter()

    def create_filter(self):
        def lt(x):
            return True if x.index[-1].to_pydatetime() < self.level else False 

        def leq(x):
            return True if x.index[-1].to_pydatetime() <= self.level else False 

        def gt(x):
            return True if x.index[-1].to_pydatetime() > self.level else False 
            
        def geq(x):
            return True if x.index[-1].to_pydatetime() >= self.level else False 
        
        def eq(x):
            return True if x.index[-1].to_pydatetime() == self.level else False 

        filter_map = {'<=': leq,
                      '>=': geq,
                      '<': lt,
                      '>': gt,
                      '=': eq,}
        return filter_map.get(self.condition.lower().strip(), None)

    def filter(self):        
        return self.filter
    
    def __str__(self):
        return f'datetimeIndex{self.condition}{self.level}'

class BaseClass(object):
    def __init__(self, **kwargs):
        super().__init__()

    def compare_timeframe(self, tf1, tf2):
        t1 = self.sanitize_timeframe(tf1)
        t2 = self.sanitize_timeframe(tf2)
        if t1.endswith('n'):
            if t2.endswith('n'): #Both in minutes
                if int(t1[0:-3])>int(t2[0:-3]):
                    return -1
                elif int(t1[0:-3])==int(t2[0:-3]):
                    return 0
                else:
                    return 1
            else: #t2 is > min, hence greater
                return 1
        elif t2.endswith('n'): #t2 in min and t1 not
            return -1

    def sanitize_timeframe(self, timeframe):
        if isinstance(timeframe, str) and timeframe[-1] not in ['m', 'M', 'h', 'H', 'W', 'D', 'd', 'w']:
            if timeframe.endswith(tuple(['min', 'Min'])):
                if timeframe[0:-3].isnumeric():
                    if int(timeframe[0:-3]) < 60:
                        return f'{timeframe[0:-3]}Min'
                    if int(timeframe[0:-3]) < 60*24:
                        return f'{int(timeframe[0:-3])//60}H'
                    if int(timeframe[0:-3]) < 60*24*7:
                        return f'{int(timeframe[0:-3])//(60*7)}D'
                    if int(timeframe[0:-3]) < 60*24*30:
                        return f'{int(timeframe[0:-3])//(60*30)}W'
                    if int(timeframe[0:-3]) >= 60*24*30:
                        return f'{int(timeframe[0:-3])//(60*30)}M'
                return timeframe
            log(f'Timeframe "{timeframe[-1]}" cannot be interpreted')
        elif not isinstance(timeframe, str):
            if isinstance(timeframe, int):
                if timeframe < 60:
                    return f'{timeframe}Min'
                if timeframe < 60*24:
                    return f'{timeframe//60}H'
                if timeframe < 60*24*7:
                    return f'{timeframe//(60*7)}D'
                if timeframe < 60*24*30:
                    return f'{timeframe//(60*30)}W'
                if timeframe >= 60*24*30:
                    return f'{timeframe//(60*30)}M'
            else:
                log(f'Timeframe "{timeframe[-1]}" must be a string')
        else:
            if timeframe[0:-1].isnumeric():
                if timeframe[-1] == 'm':
                    if int(timeframe[0:-1]) < 60:
                        return f'{timeframe[0:-1]}Min'
                    if int(timeframe[0:-1]) < 60*24:
                        return f'{int(timeframe[0:-1])//60}H'
                    if int(timeframe[0:-1]) < 60*24*7:
                        return f'{int(timeframe[0:-1])//(60*7)}D'
                    if int(timeframe[0:-1]) < 60*24*30:
                        return f'{int(timeframe[0:-1])//(60*30)}W'
                    if int(timeframe[0:-1]) >= 60*24*30:
                        return f'{int(timeframe[0:-1])//(60*30)}M'
                if timeframe[-1] in ['h', 'H']:
                    if int(timeframe[0:-1]) < 24:
                        return f'{timeframe[0:-1]}H'
                    if int(timeframe[0:-1]) < 24*7:
                        return f'{int(timeframe[0:-1])//24}D'
                    if int(timeframe[0:-1]) < 24*30:
                        return f'{int(timeframe[0:-1])//(24*7)}W'
                    if int(timeframe[0:-1]) >= 24*30:
                        return f'{int(timeframe[0:-1])//(24*30)}M'
                if timeframe[-1] in ['d', 'D']:
                    if int(timeframe[0:-1]) < 7:
                        return f'{timeframe[0:-1]}D'
                    if int(timeframe[0:-1]) <= 30:
                        return f'{int(timeframe[0:-1])//7}W'
                    if int(timeframe[0:-1]) > 30:
                        return f'{int(timeframe[0:-1])//(30)}M'
                if timeframe[-1] in ['w', 'W']:
                    if int(timeframe[0:-1]) <= 5:
                        return f'{int(timeframe[0:-1])}W'
                    if int(timeframe[0:-1]) > 5:
                        return f'{int(timeframe[0:-1])//(5)}M'
                if timeframe[-1] in ['M']:
                    return f'{timeframe}'
