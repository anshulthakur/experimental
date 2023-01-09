from tradebot.base import FlowGraphNode
from lib.logging import log
import numpy as np

class EvolvingSupportResistance(FlowGraphNode):
    def __init__(self, resistance_basis='close', support_basis='close', **kwargs):
        self.resistances = []
        self.supports = []
        self.direction = 'UNKNOWN'
        self.resistance_basis = resistance_basis
        self.support_basis = support_basis
        super().__init__(**kwargs)

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
                log(f'New support: {self.supports[-1]}', 'debug')
                s_df['support'] = df[self.support_basis][-2]
            elif (len(self.resistances)>0) and (self.direction == 'UP') and (df['close'][-1] > self.resistances[-1][1]):
                #Crossed resistance. Mark it as support
                last_resistance = self.resistances.pop()
                self.supports.append(last_resistance)
                log(f'New resistance turned support: {self.supports[-1]}', 'debug')
                s_df['support'] = last_resistance[1]
            elif (self.direction == 'DOWN'):
                #Could be turning around, mark support
                self.supports.append((df.index[-2], df[self.support_basis][-2]))
                self.direction = 'UP'
                log(f'New support: {self.supports[-1]}', 'debug')
                s_df['support'] = df[self.support_basis][-2]
        elif df['close'][-1] < df['close'][-2]:
            if len(self.resistances)==0:
                #Opening downtrend. Mark as first resistance
                self.resistances.append((df.index[-2], df[self.resistance_basis][-2]))
                self.direction = 'DOWN'
                log(f'New resistance: {self.resistances[-1]}', 'debug')
                s_df['resistance'] = df[self.resistance_basis][-2]
            elif self.direction == 'UP':
                #Price changed direction, mark resistance
                self.resistances.append((df.index[-2], df[self.resistance_basis][-2]))
                self.direction = 'DOWN'
                log(f'New resistance: {self.resistances[-1]}', 'debug')
                s_df['resistance'] = df[self.resistance_basis][-2]
            elif (self.direction == 'DOWN') and len(self.supports)>0 and (df['close'][-1] < self.supports[-1][1]):
                #Downtrend broke support, mark it as resistance now
                last_support = self.supports.pop()
                self.resistances.append(last_support)
                log(f'New resistance from support: {self.resistances[-1]}', 'debug')
                s_df['resistance'] = last_support[1]
        for node,connection in self.connections:
            await node.next(connection=connection, data = s_df.copy())
        self.consume()