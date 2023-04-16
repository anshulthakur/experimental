from tradebot.base import FlowGraphNode
from lib.logging import log
import numpy as np
from tradebot.base.signals import Resistance, Support, EndOfData

from tradebot.base.trading import BaseBot, Broker
import datetime

class EvolvingSupportResistance(FlowGraphNode):
    '''
    This strategy is fairly simple. It defines points as resistance and supports as:

    Resistance: Spot Price from where a scrip whose CMP was rising over a few periods starts to recede lower 
    Support: Spot Price from where a scrip whose CMP was falling over a few periods starts to rise higher

    From the point of start, it judges the direction of movement based on a consecutive delta. 
    So, if P_t(1) > P_t(2), t(1)<t(2): We start with a downtrend. The place where P_t(n) < P_t_(n+1) happens is 
    marked as support. Symmetrically, resistances.

    If the next resistance value (after a downtrend changes to uptrend) is taken out, it becomes support 
    until broken again. Similarly for supports turning into resistance.

    It is very naive as it doesn't look at long term horizons or the frequency with which the support/resistance
    were hit/taken out.

    It emits a signal with new support/resistance values every time they are created. However, it does not emit
    a signal that a support is converted to  resistance (delete support, add resistance), and vice versa.
    '''
    def __init__(self, resistance_basis='close', support_basis='close', **kwargs):
        super().__init__(signals= [Resistance, Support], **kwargs)
        self.resistances = []
        self.supports = []
        self.direction = 'UNKNOWN'
        self.resistance_basis = resistance_basis
        self.support_basis = support_basis

    async def next(self, connection=None, **kwargs):
        if not self.ready(connection, **kwargs):
            log(f'{self}: Not ready yet', 'debug')
            return
        df = kwargs.get('data')
        s_df = df.iloc[-2:].copy(deep=True)
        s_df['support'] = np.NaN
        s_df['resistance'] = np.NaN
        if len(df) <= 1:
            #log(f"First candle. Supports: {self.supports}. Resistances: {self.resistances}")
            return
        if df['close'][-1] > df['close'][-2]:
            if len(self.supports)==0:
                #Moving up for the first time. Mark support
                self.supports.append((df.index[-2], df[self.support_basis][-2]))
                self.direction = 'UP'
                #log(f'New support: {self.supports[-1]}', 'debug')
                s_df['support'] = df[self.support_basis][-2]
                await self.emit(Support(index=df.index[-2], value=df[self.support_basis][-2], timestamp=df.index[-1]))
            elif (len(self.resistances)>0) and (self.direction == 'UP') and (df['close'][-1] > self.resistances[-1][1]):
                #Crossed resistance. Mark it as support
                last_resistance = self.resistances.pop()
                self.supports.append(last_resistance)
                #log(f'New resistance turned support: {self.supports[-1]}', 'debug')
                s_df['support'] = last_resistance[1]
                await self.emit(Support(index=last_resistance[0], value=last_resistance[1], timestamp=df.index[-1]))
            elif (self.direction == 'DOWN'):
                #Could be turning around, mark support
                self.supports.append((df.index[-2], df[self.support_basis][-2]))
                self.direction = 'UP'
                #log(f'New support: {self.supports[-1]}', 'debug')
                s_df['support'] = df[self.support_basis][-2]
                await self.emit(Support(index=df.index[-2], value=df[self.support_basis][-2], timestamp=df.index[-1]))
        elif df['close'][-1] < df['close'][-2]:
            if len(self.resistances)==0:
                #Opening downtrend. Mark as first resistance
                self.resistances.append((df.index[-2], df[self.resistance_basis][-2]))
                self.direction = 'DOWN'
                #log(f'New resistance: {self.resistances[-1]}', 'debug')
                s_df['resistance'] = df[self.resistance_basis][-2]
                await self.emit(Resistance(index=df.index[-2], value=df[self.resistance_basis][-2], timestamp=df.index[-1]))
            elif self.direction == 'UP':
                #Price changed direction, mark resistance
                self.resistances.append((df.index[-2], df[self.resistance_basis][-2]))
                self.direction = 'DOWN'
                #log(f'New resistance: {self.resistances[-1]}', 'debug')
                s_df['resistance'] = df[self.resistance_basis][-2]
                await self.emit(Resistance(index=df.index[-2], value=df[self.resistance_basis][-2], timestamp=df.index[-1]))
            elif (self.direction == 'DOWN') and len(self.supports)>0 and (df['close'][-1] < self.supports[-1][1]):
                #Downtrend broke support, mark it as resistance now
                last_support = self.supports.pop()
                self.resistances.append(last_support)
                #log(f'New resistance from support: {self.resistances[-1]}', 'debug')
                s_df['resistance'] = last_support[1]
                await self.emit(Resistance(index=last_support[0], value=last_support[1], timestamp=df.index[-1]))
        for node,connection in self.connections:
            await node.next(connection=connection, data = s_df.copy())
        self.consume()

class Zigzag(FlowGraphNode):
    '''
    This strategy has a bit more memory and classifies points as HH-HL, LH-LL.
    Based on that, it generates Support and Resistance signals. 
    
    It can also (but doesn't) generate trend change signals right now.
    '''
    def __init__(self, **kwargs):
        super().__init__(signals= [Resistance, Support], strict=False, **kwargs)
        self.multi_input = True
        self.nearest_low = 0
        self.nearest_low_val = 0
        self.nearest_high = 0
        self.nearest_high_val = 0
        self.trend = 0 #We don't know at the outset (0: No trend, 1: Uptrend, -1: Downtrend)
    
    def get_trend_str(self, trend):
        if trend==0:
            return 'None'
        if trend==1:
            return 'Uptrend'
        if trend==-1:
            return 'Downtrend'
    def log_trendchange(self, trend, tup):
        if self.trend != trend:
            log(f'[{tup[2]}] Trend changing from {self.get_trend_str(self.trend)} to {self.get_trend_str(trend)}', 'debug')
    
    async def next(self, connection=None, **kwargs):
        #log(f'{self}: {kwargs.get("data")}', 'debug')
        if not self.ready(connection, **kwargs):
            #log(f'{self}: Not ready yet', 'debug')
            return
        #First, we'll need to figure out which input is HH/HL data and which is dataframe
        df = None
        hh_idx = None
        hl_idx = None
        lh_idx = None
        ll_idx = None
        for inp in self.inputs:
            if type(self.inputs[inp]).__name__ == 'DataFrame':
                df = self.inputs[inp]
            elif type(self.inputs[inp]).__name__ == 'dict':
                if 'HH' in self.inputs[inp]:
                    hh_idx = self.inputs[inp]['HH']
                if 'HL' in self.inputs[inp]:
                    hl_idx = self.inputs[inp]['HL']
                if 'LH' in self.inputs[inp]:
                    lh_idx = self.inputs[inp]['LH']
                if 'LL' in self.inputs[inp]:
                    ll_idx = self.inputs[inp]['LL']
            else:
                log(f'{type(self.inputs[inp]).__name__}', 'debug')
                raise Exception('Unknown input received')
        if None in [hh_idx, hl_idx, lh_idx, ll_idx]:
            log('One of HH/HL/LH/LL values are not provided.', 'debug')
            raise Exception('One of HH/HL/LH/LL values are not provided.')
        #Determine the nearest high/low values
        emit_sig = False
        if len(hh_idx)> 0:
            if hh_idx[-1] > self.nearest_high:
                self.nearest_high = hh_idx[-1]
                self.nearest_high_val = df.iloc[hh_idx[-1]]['close']
                emit_sig = True
        if len(lh_idx)> 0:
            if lh_idx[-1] > self.nearest_high:
                self.nearest_high = lh_idx[-1]
                self.nearest_high_val = df.iloc[lh_idx[-1]]['close']
                emit_sig = True
        if emit_sig:
            if df.iloc[-1]['close']<self.nearest_high_val:
                #We're still below the last high, so that's a resistance
                log(f"New Resistance: {df.iloc[self.nearest_high]['close']} [{df.iloc[self.nearest_high].name}]", "debug")
                await self.emit(Resistance(index=df.iloc[self.nearest_high].name, 
                                            value=df.iloc[self.nearest_high]['close'], 
                                            timestamp=df.index[-1]))
        emit_sig = False
        if len(ll_idx)> 0:
            if ll_idx[-1] > self.nearest_low:
                self.nearest_low = ll_idx[-1]
                self.nearest_low_val = df.iloc[ll_idx[-1]]['close']
                emit_sig = True
        if len(hl_idx)> 0:
            if hl_idx[-1] > self.nearest_low:
                self.nearest_low = hl_idx[-1]
                self.nearest_low_val = df.iloc[hl_idx[-1]]['close']
                emit_sig = True
        if emit_sig:
            if df.iloc[-1]['close']>self.nearest_low_val:
                #We're still above the last low, so that's a support
                log(f"New Support: {df.iloc[self.nearest_low]['close']} [{df.iloc[self.nearest_low].name}]", "debug")
                await self.emit(Support(index=df.iloc[self.nearest_low].name, 
                                            value=df.iloc[self.nearest_low]['close'], 
                                            timestamp=df.index[-1]))
        #Also determine trend. Here, we take the rule as gospel and do not worry about micro-trends within trends (for that we have to look at a smaller TF)
        # If we are making a HH-HL, it is uptrend. 
        # If LH-LL, downtrend
        # Else, not known
        #Line up all LH/LL/HH/HL into a single array and look into it
        trend_array = []
        for idx in hh_idx:
            trend_array.append(('HH', idx, df.iloc[idx].name, df.iloc[idx]['close']))
        for idx in hl_idx:
            trend_array.append(('HL', idx, df.iloc[idx].name, df.iloc[idx]['close']))
        for idx in lh_idx:
            trend_array.append(('LH', idx, df.iloc[idx].name, df.iloc[idx]['close']))
        for idx in ll_idx:
            trend_array.append(('LL', idx, df.iloc[idx].name, df.iloc[idx]['close']))
        trend_array.sort(key=lambda x: x[1])

        #Now, look at the last entries
        if len(trend_array)<2: #Need at least 2 data points for comparison
            self.consume()
            return

        if trend_array[-2][0] == 'LL':
            if trend_array[-1][0] == 'LL': #Missed a LH due to window size?
                self.log_trendchange(-1, trend_array[-1])
                self.trend = -1
            elif trend_array[-1][0] == 'LH': #LL-LH: Still a downtrend. Could change.
                self.log_trendchange(-1, trend_array[-1])
                self.trend = -1
            elif trend_array[-1][0] == 'HH': #Whipsawing (HH-HL-HH-LL-HH)
                self.log_trendchange(0, trend_array[-1])
                self.trend = 0
            elif trend_array[-1][0] == 'HL': #Missed a LH due to window size?
                self.log_trendchange(0, trend_array[-1])
                self.trend = 0
        elif trend_array[-2][0] == 'HH':
            if trend_array[-1][0] == 'LL': #Whipsawing (HH-HL-HH-LL)
                self.log_trendchange(0, trend_array[-1])
                self.trend = 0
            elif trend_array[-1][0] == 'LH': #Missed a HL due to window size?
                self.log_trendchange(0, trend_array[-1])
                self.trend = 0
            elif trend_array[-1][0] == 'HH': #Missed a HL due to window size?
                self.log_trendchange(1, trend_array[-1])
                self.trend = 1
            elif trend_array[-1][0] == 'HL': #Still an uptrend. Could change
                self.log_trendchange(1, trend_array[-1])
                self.trend = 1
        elif trend_array[-2][0] == 'HL':
            if trend_array[-1][0] == 'LL': #Missed a LH due to window size? Trend changed
                self.log_trendchange(-1, trend_array[-1])
                self.trend = -1
            elif trend_array[-1][0] == 'LH': #Could be changing trends
                self.log_trendchange(0, trend_array[-1])
                self.trend = 0
            elif trend_array[-1][0] == 'HH': #Uptrend intact (trend continuation), or changed
                self.log_trendchange(1, trend_array[-1])
                self.trend = 1
            elif trend_array[-1][0] == 'HL': #Missed a LH due to window size? Trend intact
                self.log_trendchange(1, trend_array[-1])
                self.trend = 1
        elif trend_array[-2][0] == 'LH':
            if trend_array[-1][0] == 'LL': #Trend continuation or changed
                self.log_trendchange(-1, trend_array[-1])
                self.trend = -1
            elif trend_array[-1][0] == 'LH': #Missed a LL due to window size
                self.log_trendchange(-1, trend_array[-1])
                self.trend = -1
            elif trend_array[-1][0] == 'HH': #Missed a LL/HL due to window size
                self.log_trendchange(1, trend_array[-1])
                self.trend = 1
            elif trend_array[-1][0] == 'HL': #Might be turning a corner
                self.log_trendchange(0, trend_array[-1])
                self.trend = 0

        
        for node,connection in self.connections:
            await node.next(connection=connection, data = trend_array)

        self.consume()

class LongBot(FlowGraphNode, BaseBot, Broker):
    '''
    This is a bot which only goes long and quits if stop loss is hit.
    The signals may be coming from any of the price-action nodes. 
    It consumes Resistance and Support signals right now.
    '''
    def __init__(self, **kwargs):
        self.sl = None
        self.resistance = None
        self.support = None
        super().__init__(**kwargs)
    
    async def next(self, connection=None, **kwargs):
        if not self.ready(connection, **kwargs):
            log(f'{self}: Not ready yet', 'debug')
            return
        df = kwargs.get('data')
        if self.position and self.sl > df['close'][-1]:
            self.close_position(df['close'][-1], date=df.index[-1].to_pydatetime())
            self.sl = 0
            log(f'SL Hit {df["close"][-1]}. Total Charges (so far): {self.charges}', 'info')
        if self.resistance is not None and df['close'][-1] > self.resistance:
            self.resistance = None
            if not self.position:
                #Go long if we are above resistance
                self.sl = self.support
                self.buy(df['close'][-1], date=df.index[-1].to_pydatetime())
                log(f"Go long: {df['close'][-1]} SL: {self.sl}", 'info')
        self.consume()

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
        else:
            log(f"Unknown signal {signal.name()}")

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
    
    async def next(self, connection=None, **kwargs):
        if not self.ready(connection, **kwargs):
            log(f'{self}: Not ready yet', 'debug')
            return
        df = kwargs.get('data')
        if self.position and self.close_orderbook(df):
            self.close_position(df.iloc[-1]['close'], date=df.index[-1].to_pydatetime())
            self.sl = None
            log(f"Trade closed {df.iloc[-1]['close']}. Total Charges (so far): {self.charges}", 'info')

        elif self.position and self.sl <= df.iloc[-1]['close']:
            self.close_position(df.iloc[-1]['close'], date=df.index[-1].to_pydatetime())
            self.sl = None
            log(f"SL Hit {df.iloc[-1]['close']}. Total Charges (so far): {self.charges}", 'info')

        elif df.iloc[-1]['close'] <= df.iloc[-1]['EMA'+self.ema_val]:
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
    def __init__(self, value=20, proximity=1.0, **kwargs):
        self.sl = None
        self.ema_val = str(value)
        self.proximity = proximity
        self.last_close = 0
        self.ticks_since_last_touch = 0 #In case we want to incorporate rebounce to avoid taking positions in noisy environment
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
        if self.position and self.sl >= df.iloc[-1]['close']:
            self.close_position(df.iloc[-1]['close'], date=df.index[-1].to_pydatetime())
            self.sl = None
            log(f'SL Hit {df.iloc[-1]["close"]}. Total Charges (so far): {self.charges}', 'info')

        if df.iloc[-1]['close'] >= df.iloc[-1]['EMA'+self.ema_val]:
            #Still above EMA. Are we in proximity?
            if (abs(df.iloc[-1]['close'] - df.iloc[-1]['EMA'+self.ema_val])/df.iloc[-1]['EMA'+self.ema_val])*100 <= self.proximity:
                #We are within proximity. Go short if there isn't a position open, else, update SL
                if not self.position:
                    self.sell(df.iloc[-1]['close'], date=df.index[-1].to_pydatetime())
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
        else:
            log(f"Unknown signal {signal.name()}")