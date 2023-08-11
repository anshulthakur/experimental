from tradebot.base import FlowGraphNode
from lib.logging import log
import numpy as np
from tradebot.base.signals import Resistance, Support, EndOfData, Shutdown

from tradebot.base.trading import BaseBot, Broker
import datetime

class LongBot(FlowGraphNode, BaseBot, Broker):
    '''
    This is a bot which only goes long and quits if stop loss is hit.
    The signals may be coming from any of the price-action nodes. 
    It consumes Resistance and Support signals right now.
    '''
    def __init__(self, overnight_positions=False, last_candle_time='15:15:00', **kwargs):
        self.sl = None
        self.resistance = None
        self.support = None
        self.overnight_positions = overnight_positions
        self.last_candle_time = datetime.datetime.strptime(last_candle_time, "%H:%M:%S")
        super().__init__(**kwargs)

    def close_orderbook(self, df):
        if not self.overnight_positions:
            last_candle = df.index[-1].to_pydatetime()
            #log(f'{last_candle.hour}=={self.last_candle_time.hour}, {last_candle.minute}== {self.last_candle_time.minute}')
            if (last_candle.hour == self.last_candle_time.hour) and (last_candle.minute >= self.last_candle_time.minute):
                return True
        return False
    
    async def next(self, connection=None, **kwargs):
        if not self.ready(connection, **kwargs):
            log(f'{self}: Not ready yet', 'debug')
            return
        df = kwargs.get('data')
        if self.position and self.close_orderbook(df):
            self.close_position(df.iloc[-1]['close'], date=df.index[-1].to_pydatetime())
            self.sl = None
            log(f"Trade closed {df.iloc[-1]['close']}. Total Charges (so far): {self.charges}", 'info')
        elif self.position and self.sl > df['close'][-1]:
            self.close_position(df['close'][-1], date=df.index[-1].to_pydatetime())
            self.sl = 0
            log(f'SL Hit {df["close"][-1]}. Total Charges (so far): {self.charges}', 'info')
        elif self.resistance is not None and df['close'][-1] > self.resistance:
            self.resistance = None
            if not self.position:
                #Go long if we are above resistance
                self.sl = self.support
                self.buy(df['close'][-1], date=df.index[-1].to_pydatetime())
                log(f"Go long: {df['close'][-1]} SL: {self.sl}", 'info')
        self.consume()
        return

    async def handle_signal(self, signal):
        if signal.name() == Resistance.name():
            self.resistance = signal.value
        elif signal.name() == Support.name():
            self.support = signal.value
            if self.position:
                self.sl = self.support
                log(f'Update stop loss: {self.sl}', 'info')
        elif signal.name() == EndOfData.name():
            self.summary()
            self.get_orderbook()
            self.save_orderbook()
            self.save_tradebook()
        elif signal.name() == Shutdown.name():
            log('Received shutdown signal', 'debug')
            self.summary()
            self.save_orderbook()
            self.save_tradebook()
        else:
            log(f"Unknown signal {signal.name()}")
        #log('Bot Returning', 'debug')
        return

class DynamicResistanceBot(FlowGraphNode, BaseBot, Broker):
    '''
    This is a bot which only goes short and quits if stop loss is hit.
    There are no signals to work on, it is fed data enriched with the EMA
    values. 
    If we are in downtrend, and price (high) in the last candle is within a proximity of the 
    tracked resistance without crossing over, go short.
    Stop loss is fixed points above for now (high of last candle). Later, it may be the nearest resistance.
    '''
    def __init__(self, value=20, proximity=1.0, overnight_positions=False, last_candle_time='15:15:00', **kwargs):
        self.sl = None
        self.ema_val = str(value)
        self.proximity = proximity
        self.last_close = 0
        self.ticks_since_last_touch = 0 #In case we want to incorporate rebounce to avoid taking positions in noisy environment
        self.overnight_positions = overnight_positions
        self.last_candle_time = datetime.datetime.strptime(last_candle_time, "%H:%M:%S")
        super().__init__(**kwargs)
    
    def close_orderbook(self, df):
        if not self.overnight_positions:
            last_candle = df.index[-1].to_pydatetime()
            #log(f'{last_candle.hour}=={self.last_candle_time.hour}, {last_candle.minute}== {self.last_candle_time.minute}')
            if (last_candle.hour == self.last_candle_time.hour) and (last_candle.minute >= self.last_candle_time.minute):
                return True
        return False
    
    def taking_fresh_orders(self, candle_time):
        #log(f'{last_candle.hour}=={self.last_candle_time.hour}, {last_candle.minute}== {self.last_candle_time.minute}')
        if (candle_time.hour >= self.last_candle_time.hour) and (candle_time.minute >= self.last_candle_time.minute):
            return False
        return True
    
    async def next(self, connection=None, **kwargs):
        if not self.ready(connection, **kwargs):
            log(f'{self}: Not ready yet', 'debug')
            return
        df = kwargs.get('data')
        if self.position and self.close_orderbook(df):
            self.close_position(df.iloc[-1]['close'], date=df.index[-1].to_pydatetime())
            self.sl = None
            log(f"Trade closed {df.iloc[-1]['close']}. Total Charges (so far): {self.charges}", 'info')

        elif self.taking_fresh_orders(candle_time=df.index[-1].to_pydatetime()) and self.position and self.sl <= df.iloc[-1]['close']:
            self.close_position(df.iloc[-1]['close'], date=df.index[-1].to_pydatetime())
            self.sl = None
            log(f"SL Hit {df.iloc[-1]['close']}. Total Charges (so far): {self.charges}", 'info')

        elif self.taking_fresh_orders(candle_time=df.index[-1].to_pydatetime()) and df.iloc[-1]['close'] <= df.iloc[-1]['EMA'+self.ema_val]:
            #Still below EMA. Are we in proximity?
            if (abs(df.iloc[-1]['close'] - df.iloc[-1]['EMA'+self.ema_val])/df.iloc[-1]['EMA'+self.ema_val])*100 <= self.proximity:
                #We are within proximity. Go short if there isn't a position open, else, update SL
                if not self.position:
                    self.sell(df.iloc[-1]['close'], date=df.index[-1].to_pydatetime())
                    self.sl = df.iloc[-1]['high']
                    log(f"Go short: {df.iloc[-1]['close']} SL: {self.sl}", 'info')
                else:
                    self.sl = df.iloc[-1]['high']
        self.last_close = [df.iloc[-1]['close'], df.index[-1]]
        self.consume()

    async def handle_signal(self, signal):
        if signal.name() == EndOfData.name():
            if self.position:
                self.close_position(self.last_close[0], date=self.last_close[1].to_pydatetime())
            self.summary()
            self.get_orderbook()
            self.save_orderbook()
            self.save_tradebook()
        elif signal.name() == Shutdown.name():
            log('Received shutdown signal', 'debug')
            self.summary()
            self.save_orderbook()
            self.save_tradebook()
        else:
            log(f"Unknown signal {signal.name()}")

class DynamicSupportBot(FlowGraphNode, BaseBot, Broker):
    '''
    This is a bot which only goes long and quits if stop loss is hit.
    There are no signals to work on, it is fed data enriched with the EMA
    values. 
    If we are in uptrend, and price (low) in the last candle is within a proximity of the 
    tracked support without crossing over, go long.
    Stop loss is fixed points above for now (low of last candle). Later, it may be the nearest support.
    '''
    def __init__(self, value=20, proximity=1.0, overnight_positions=False, last_candle_time='15:15:00', **kwargs):
        self.sl = None
        self.ema_val = str(value)
        self.proximity = proximity
        self.last_close = 0
        self.ticks_since_last_touch = 0 #In case we want to incorporate rebounce to avoid taking positions in noisy environment
        self.overnight_positions = overnight_positions
        self.last_candle_time = datetime.datetime.strptime(last_candle_time, "%H:%M:%S")
        super().__init__(**kwargs)
    
    def close_orderbook(self, df):
        if not self.overnight_positions:
            last_candle = df.index[-1].to_pydatetime()
            #log(f'{last_candle.hour}=={self.last_candle_time.hour}, {last_candle.minute}== {self.last_candle_time.minute}')
            if (last_candle.hour == self.last_candle_time.hour) and (last_candle.minute >= self.last_candle_time.minute):
                return True
        return False
    
    def taking_fresh_orders(self, candle_time):
        #log(f'{last_candle.hour}=={self.last_candle_time.hour}, {last_candle.minute}== {self.last_candle_time.minute}')
        if (candle_time.hour >= self.last_candle_time.hour) and (candle_time.minute >= self.last_candle_time.minute):
            return False
        return True
    
    async def next(self, connection=None, **kwargs):
        if not self.ready(connection, **kwargs):
            log(f'{self}: Not ready yet', 'debug')
            return
        df = kwargs.get('data')
        #log(f'{df.tail(1)}')
        if self.position and self.close_orderbook(df):
            self.close_position(df.iloc[-1]['close'], date=df.index[-1].to_pydatetime())
            self.sl = None
            log(f"Trade closed {df.iloc[-1]['close']}. Total Charges (so far): {self.charges}", 'info')

        elif self.taking_fresh_orders(candle_time=df.index[-1].to_pydatetime()) and self.position and self.sl >= df.iloc[-1]['close']:
            self.close_position(df.iloc[-1]['close'], date=df.index[-1].to_pydatetime())
            self.sl = None
            log(f'SL Hit {df.iloc[-1]["close"]}. Total Charges (so far): {self.charges}', 'info')

        elif self.taking_fresh_orders(candle_time=df.index[-1].to_pydatetime()) and df.iloc[-1]['close'] >= df.iloc[-1]['EMA'+self.ema_val]:
            #Still above EMA. Are we in proximity?
            if (abs(df.iloc[-1]['close'] - df.iloc[-1]['EMA'+self.ema_val])/df.iloc[-1]['EMA'+self.ema_val])*100 <= self.proximity:
                #We are within proximity. Go long if there isn't a position open, else, update SL
                if not self.position:
                    self.buy(df.iloc[-1]['close'], date=df.index[-1].to_pydatetime())
                    self.sl = df.iloc[-1]['low']
                    log(f"Go long: {df.iloc[-1]['close']} SL: {self.sl}", 'info')
                else:
                    self.sl = df.iloc[-1]['low']
        self.last_close = [df.iloc[-1]['close'], df.index[-1]]
        self.consume()

    async def handle_signal(self, signal):
        if signal.name() == EndOfData.name():
            if self.position:
                self.close_position(self.last_close[0], date=self.last_close[1].to_pydatetime())
            self.summary()
            self.get_orderbook()
            self.save_orderbook()
            self.save_tradebook()
        elif signal.name() == Shutdown.name():
            log('Received shutdown signal', 'debug')
            self.summary()
            self.save_orderbook()
            self.save_tradebook()
        else:
            log(f"Unknown signal {signal.name()}")

