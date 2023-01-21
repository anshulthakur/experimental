from tradebot.base import FlowGraphNode
from lib.logging import log

from tradebot.base.signals import *
import pandas as pd 

class Union(FlowGraphNode):
    def __init__(self, columnmap={}, **kwargs):
        self.multi_input = True
        self.columnmap = columnmap
        super().__init__(**kwargs)

    async def next(self, connection=None, **kwargs):
        if not self.ready(connection, **kwargs):
            log(f'{self}: Not ready yet', 'debug')
            return
        log(f'{self}:', 'debug')
        if self.input_types == 'DataFrame':
            df = None
            for conn in self.inputs:
                if df is None:
                    df = self.inputs[conn]
                else:
                    df.merge(self.inputs[conn], how='outer')
                #log(f'{conn}', 'debug')
                log(f'{df.tail(1)}', 'debug')
        elif self.input_types == 'list':
            df = []
            for conn in self.inputs:
                df = list(set(df) | set(self.inputs[conn]))
                #log(f'{conn}', 'debug')
                log(f'{df}', 'debug')
        for node,connection in self.connections:
            await node.next(connection=connection, data = df.copy())
        self.consume()
        return

class Intersection(FlowGraphNode):
    def __init__(self, **kwargs):
        self.multi_input = True
        super().__init__(**kwargs)

    async def next(self, connection=None, **kwargs):
        if not self.ready(connection, **kwargs):
            log(f'{self}: Not ready yet', 'debug')
            return
        log(f'{self}:', 'debug')
        if self.input_types == 'DataFrame':
            df = None
            for conn in self.inputs:
                if df is None:
                    df = self.inputs[conn]
                else:
                    df.merge(self.inputs[conn], how='inner')
                #log(f'{conn}', 'debug')
                log(f'{df.tail(1)}', 'debug')
        elif self.input_types == 'list':
            df = []
            for conn in self.inputs:
                df = list(set(df) & set(self.inputs[conn]))
                #log(f'{conn}', 'debug')
                log(f'{df}', 'debug')
        for node,connection in self.connections:
            await node.next(connection=connection, data = df.copy())
        self.consume()
        return

class ColumnFilter(FlowGraphNode):
    def __init__(self, map={}, **kwargs):
        self.multi_input = False
        if len(map)==0:
            raise Exception("Please provide the input-to-output column mapping")
        self.map = map
        super().__init__(**kwargs)

    async def next(self, connection=None, **kwargs):
        if not self.ready(connection, **kwargs):
            log(f'{self}: Not ready yet', 'debug')
            return
        #log(f'{self}:', 'debug')
        if self.input_types == 'DataFrame':
            for conn in self.inputs:
                df = self.inputs[conn]
                columns = list(df.columns)
                dropcolumns = []
                for column in self.map:
                    if column not in columns:
                        raise Exception(f"{column} not in incoming dataframe")
                for column in columns:
                    if column not in self.map:
                        dropcolumns.append(column)
                df = self.inputs[conn].rename(columns=self.map)
                df.drop(columns=dropcolumns, inplace=True)
                #log(f'{conn}', 'debug')
                #log(f'{df.tail(1)}', 'debug')
        for node,connection in self.connections:
            await node.next(connection=connection, data = df.copy())
        self.consume()
        return
