from tradebot.base import FlowGraphNode, TimeFilter
from lib.logging import log
import pandas as pd
from lib.pivots import getHHIndex, getHLIndex, getLHIndex, getLLIndex
import json
from base.signals import Alert

class MinMaxDetector(FlowGraphNode):
    def __init__(self, lookaround=1, **kwargs):
        self.order = lookaround #Lookaround width in which peak must be detected. Minimum should be 1 (creates vote in 3)
        super().__init__(**kwargs)
    
    async def next(self, connection=None, **kwargs):
        '''
        Rather than passing the entire history explicitly, we could simply emit a signal whenever a new
        Local Maxima/Minima is detected. Some other node can do the task of cataloguing the values and 
        using them as per their requirement
        '''
        if not self.ready(connection, **kwargs):
            log(f'{self}: Not ready yet', 'debug')
            return
        df = kwargs.get('data')
        #log(f'{self}: {df.tail(1)}', 'debug')
        
        data = {'HH': [],
                'HL': [],
                'LH': [],
                'LL': []}

        if len(list(df.columns))==1:
            #Find peaks and valleys over the given single column data
            for h_idx in getHHIndex(df['close'].values, order=self.order):
                data['HH'].append(h_idx)
            for h_idx in getLHIndex(df['close'].values, order=self.order):
                data['LH'].append(h_idx)
            for l_idx in getHLIndex(df['close'].values, order=self.order):
                data['HL'].append(l_idx)
            for l_idx in getLLIndex(df['close'].values, order=self.order):
                data['LL'].append(l_idx)
            for node,connection in self.connections:
                #log(f"{self}: {data}", 'debug')
                await node.next(connection=connection, data = data)
        else:
            columns = list(df.columns)
            for node,connection in self.connections:
                log(f"{self}: {data}", 'debug')
                await node.next(connection=connection, data = data)
        self.consume()

class PriceAlerts(FlowGraphNode):
    def load_alerts(self):
        log(f'Load alerts from {self.file}')
        with open(self.file, 'r') as fd:
            watchlist = json.load(fd)
            log(f'Look for alerts in {self.timeframe} timeframe')
            if self.timeframe not in watchlist:
                #log(f'No alerts for {self.timeframe} timeframe')
                return
            alerts = watchlist[self.timeframe]
            for alert in alerts:
                #log(f'Alert {alert}: {alerts[alert]}')
                event = Alert(name=alert, 
                              scrip=alerts[alert]['scrip'],
                              key='close',
                              condition=alerts[alert]['condition'],
                              level=alerts[alert]['level'],
                              margin=alerts[alert].get('margin', 0.0001),
                              recurring=alerts[alert].get('recurring', False),
                              timeframe=self.timeframe)
                if 'after' in alerts[alert]:
                    filter = TimeFilter(value=alerts[alert]['after'], condition='>=')
                    #log('Add timefilter', 'debug')
                    event.add_filters([filter])
                self.subscribe(event=event)
        pass

    def __init__(self, file=None, **kwargs):
        super().__init__(publications=[self.sanitize_timeframe(kwargs.get('timeframe', '1Min'))], **kwargs)
        self.file = file #File from which alerts must be loaded
        if self.file is not None:
            self.load_alerts()

    async def next(self, connection=None, **kwargs):
        if not self.ready(connection, **kwargs):
            log(f'{self}: Not ready yet', 'debug')
            return
        df = kwargs.pop('data')
        #log(f'{df.tail(1)}', 'debug')
        await self.notify(df)
        for node,connection in self.connections:
            await node.next(connection=connection, data = df.copy(deep=True), **kwargs)
        self.consume()

    async def handle_event_notification(self, event):
        log(f'Event {event.name} received.', 'debug')
        log(f'{self.name}:{event.df}', 'debug')
        if not event.recurring:
            self.unsubscribe(event)
        return