
class BaseSignal(object):
    @classmethod
    def name(cls):
        raise Exception('Cannot invoke BaseSignal')

    def __init__(self, **kwargs):
        pass

    def __str__(self):
        return self.name

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

    def __str__(self):
        return f"{self.name}[{self.timestamp.to_pydatetime()}] Support at {self.value} ({self.index})"