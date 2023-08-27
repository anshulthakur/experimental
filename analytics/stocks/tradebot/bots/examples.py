from tradebot.base import FlowGraphNode
from lib.logging import log
import numpy as np
from tradebot.base.signals import Resistance, Support, EndOfData, Shutdown, Alert, EndOfDay

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
        self.busy = True
        df = kwargs.pop('data')
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
        self.busy = False
        return

    async def handle_signal(self, signal):
        self.wait_until_busy()
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
    def __init__(self, value=20, proximity=1.0, overnight_positions=False, last_candle_time='15:15:00', stop_loss_tf = '1Min', **kwargs):
        self.sl = None
        self.ema_val = str(value)
        self.proximity = proximity
        self.last_close = 0
        self.ticks_since_last_touch = 0 #In case we want to incorporate rebounce to avoid taking positions in noisy environment
        self.overnight_positions = overnight_positions
        self.last_candle_time = datetime.datetime.strptime(last_candle_time, "%H:%M:%S")
        self.tf_val = self.sanitize_timeframe(stop_loss_tf)
        super().__init__(**kwargs)
    
    def close_orderbook(self, df):
        delta = self.get_delta(timeframe=self.timeframe)
        if not self.overnight_positions:
            last_candle = df.index[-1].to_pydatetime()+delta if delta is not None else df.index[-1].to_pydatetime()
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
        self.busy = True
        df = kwargs.pop('data')
        if self.position is not None and self.close_orderbook(df):
            self.close_position(df.iloc[-1]['close'], date=df.index[-1].to_pydatetime(), timeframe=self.timeframe)
            self.sl = None
            log(f"End-of-day Trade closed {df.iloc[-1]['close']}. Total Charges (so far): {self.charges}", 'info')
            for event in self.registered_events:
                self.unsubscribe(self.registered_events[event])
            self.registered_events = {}

        elif self.taking_fresh_orders(candle_time=df.index[-1].to_pydatetime()) and self.position is not None and self.sl <= df.iloc[-1]['close']:
            self.close_position(df.iloc[-1]['close'], date=df.index[-1].to_pydatetime(), timeframe=self.timeframe)
            self.sl = None
            log(f"SL Hit {df.iloc[-1]['close']}. Total Charges (so far): {self.charges}", 'info')
            for event in self.registered_events:
                self.unsubscribe(self.registered_events[event])
            self.registered_events = {}

        elif df.iloc[-1]['close'] <= df.iloc[-1]['EMA'+self.ema_val]:
            #Still below EMA. Are we in proximity?
            if (abs(df.iloc[-1]['close'] - df.iloc[-1]['EMA'+self.ema_val])/df.iloc[-1]['EMA'+self.ema_val])*100 <= self.proximity:
                #We are within proximity. Go short if there isn't a position open, else, update SL
                if not self.position and self.taking_fresh_orders(candle_time=df.index[-1].to_pydatetime()):
                    self.sell(df.iloc[-1]['close'], date=df.index[-1].to_pydatetime(), timeframe=self.timeframe)
                    self.sl = df.iloc[-1]['high']
                    log(f'{df.tail(1)}')
                    log(f"Go short: {df.iloc[-1]['close']} SL: {self.sl}", 'info')
                    event = Alert(name='StopLoss', key='close', condition='>=', level=self.sl, recurring=False, timeframe=self.sanitize_timeframe(self.tf_val))
                    self.subscribe(event)
                    self.registered_events[event.name] = event
                elif self.position is not None:
                    self.sl = df.iloc[-1]['high']
                    #Delete previous alert and set a new one
                    for event in self.registered_events:
                        self.unsubscribe(self.registered_events[event])
                    self.registered_events = {}
                    log(f'{df.tail(1)}')
                    log(f'Update stoploss to {self.sl}')
                    event = Alert(name='StopLoss', key='close', condition='>=', level=self.sl, recurring=False, timeframe=self.sanitize_timeframe(self.tf_val))
                    self.subscribe(event)
                    self.registered_events[event.name] = event
        self.last_close = [df.iloc[-1]['close'], df.index[-1]]
        self.consume()
        self.busy = False

    async def handle_event_notification(self, event):
        log(f'Event {event.name} received.', 'debug')
        log(f'{event.df}', 'debug')
        self.wait_until_busy()
        if self.position:
            self.close_position(event.df.iloc[-1]['close'], date=event.df.index[-1].to_pydatetime(), timeframe=event.timeframe)
            self.sl = None
            #log(f"(Event) SL Hit {event.df.iloc[-1]['close']}. Total Charges (so far): {self.charges}", 'info')
            for event in self.registered_events:
                self.unsubscribe(self.registered_events[event])
            self.registered_events = {}
        return

    async def handle_signal(self, signal):
        self.wait_until_busy()
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
        elif signal.name() == EndOfDay.name():
            #log('Received EndOfDay', 'debug')
            if not self.overnight_positions:
                ret = self.compare_timeframe(signal.timeframe, self.timeframe)
                if ret <= 0:
                    self.wait_until_busy()
                    if self.position:
                        self.close_position(signal.df.iloc[-1]['close'], date=signal.df.index[-1].to_pydatetime(), timeframe=signal.timeframe)
                        self.sl = None
                        log(f"End-of-day Trade closed {signal.df.iloc[-1]['close']}. Total Charges (so far): {self.charges}", 'info')
                        for event in self.registered_events:
                            self.unsubscribe(self.registered_events[event])
                        self.registered_events = {}
            else:
                #log('Pass', 'debug')
                pass
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
    def __init__(self, value=20, proximity=1.0, overnight_positions=False, last_candle_time='15:15:00', stop_loss_tf = '1Min', **kwargs):
        self.sl = None
        self.ema_val = str(value)
        self.proximity = proximity
        self.last_close = 0
        self.ticks_since_last_touch = 0 #In case we want to incorporate rebounce to avoid taking positions in noisy environment
        self.overnight_positions = overnight_positions
        self.last_candle_time = datetime.datetime.strptime(last_candle_time, "%H:%M:%S")
        self.tf_val = self.sanitize_timeframe(stop_loss_tf)
        super().__init__(**kwargs)
    
    def close_orderbook(self, df):
        delta = self.get_delta(timeframe=self.timeframe)
        if not self.overnight_positions:
            last_candle = df.index[-1].to_pydatetime()+delta if delta is not None else df.index[-1].to_pydatetime()
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
        self.busy =True
        df = kwargs.pop('data')
        #log(f'{df.tail(1)}')
        if self.position and self.close_orderbook(df):
            self.close_position(df.iloc[-1]['close'], date=df.index[-1].to_pydatetime(), timeframe=self.timeframe)
            self.sl = None
            log(f"End-of-day Trade closed {df.iloc[-1]['close']}. Total Charges (so far): {self.charges}", 'info')
            for event in self.registered_events:
                self.unsubscribe(self.registered_events[event])
            self.registered_events = {}

        elif self.position and self.sl >= df.iloc[-1]['close']:
            self.close_position(df.iloc[-1]['close'], date=df.index[-1].to_pydatetime(), timeframe=self.timeframe)
            self.sl = None
            log(f'SL Hit {df.iloc[-1]["close"]}. Total Charges (so far): {self.charges}', 'info')
            for event in self.registered_events:
                self.unsubscribe(self.registered_events[event])
            self.registered_events = {}

        elif df.iloc[-1]['close'] >= df.iloc[-1]['EMA'+self.ema_val]:
            #Still above EMA. Are we in proximity?
            if (abs(df.iloc[-1]['close'] - df.iloc[-1]['EMA'+self.ema_val])/df.iloc[-1]['EMA'+self.ema_val])*100 <= self.proximity:
                #We are within proximity. Go long if there isn't a position open, else, update SL
                if self.position is None and self.taking_fresh_orders(candle_time=df.index[-1].to_pydatetime()) :
                    self.buy(df.iloc[-1]['close'], date=df.index[-1].to_pydatetime(), timeframe=self.timeframe)
                    self.sl = df.iloc[-1]['low']
                    log(f'{df.tail(1)}')
                    log(f"Go long: {df.iloc[-1]['close']} SL: {self.sl}", 'info')
                    event = Alert(name='StopLoss', key='close', condition='<=', level=self.sl, recurring=False, timeframe=self.sanitize_timeframe(self.tf_val))
                    self.subscribe(event)
                    self.registered_events[event.name] = event
                elif self.position is not None:
                    #log(f'{self.position}')
                    self.sl = df.iloc[-1]['low']
                    #Delete previous alert and set a new one
                    for event in self.registered_events:
                        self.unsubscribe(self.registered_events[event])
                    self.registered_events = {}
                    log(f'{df.tail(1)}')
                    log(f'Update stoploss to {self.sl}')
                    event = Alert(name='StopLoss', key='close', condition='<=', level=self.sl, recurring=False, timeframe=self.sanitize_timeframe(self.tf_val))
                    self.subscribe(event)
                    self.registered_events[event.name] = event

        self.last_close = [df.iloc[-1]['close'], df.index[-1]]
        self.consume()
        self.busy = False
    
    async def handle_event_notification(self, event):
        log(f'Event {event.name} received.', 'debug')
        log(f'{event.df}', 'debug')
        self.wait_until_busy()
        if self.position:
            self.close_position(event.df.iloc[-1]['close'], date=event.df.index[-1].to_pydatetime(), timeframe=event.timeframe)
            self.sl = None
            #log(f"(Event) SL Hit {event.df.iloc[-1]['close']}. Total Charges (so far): {self.charges}", 'info')
            for event in self.registered_events:
                self.unsubscribe(self.registered_events[event])
            self.registered_events = {}
        return

    async def handle_signal(self, signal):
        self.wait_until_busy()
        if signal.name() == EndOfData.name():
            if self.position:
                self.close_position(self.last_close[0], date=self.last_close[1].to_pydatetime(), timeframe=self.timeframe)
            self.summary()
            self.get_orderbook()
            self.save_orderbook()
            self.save_tradebook()
        elif signal.name() == Shutdown.name():
            log('Received shutdown signal', 'debug')
            self.summary()
            self.save_orderbook()
            self.save_tradebook()
        elif signal.name() == EndOfDay.name():
            #log('Received EndOfDay', 'debug')
            if not self.overnight_positions:
                ret = self.compare_timeframe(signal.timeframe, self.timeframe)
                if ret <= 0:
                    self.wait_until_busy()
                    if self.position:
                        self.close_position(signal.df.iloc[-1]['close'], date=signal.df.index[-1].to_pydatetime(), timeframe=signal.timeframe)
                        self.sl = None
                        log(f"End-of-day Trade closed {signal.df.iloc[-1]['close']}. Total Charges (so far): {self.charges}", 'info')
                        for event in self.registered_events:
                            self.unsubscribe(self.registered_events[event])
                        self.registered_events = {}
            else:
                #log('Pass', 'debug')
                pass
        else:
            log(f"Unknown signal {signal.name()}")

