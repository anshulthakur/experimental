from tradebot.base import FlowGraphNode
from lib.logging import log

from tradebot.base.signals import *
import pandas as pd 

class Union(FlowGraphNode):
    """Merge multiple dataframes of same length columnwise before 
    passing to the next node.

    It assumes that other than the index, no other columns are 
    repeated. Performs an outer join.
    """
    def __init__(self, columnmap={}, **kwargs):
        self.multi_input = True
        self.columnmap = columnmap
        super().__init__(**kwargs)

    async def next(self, connection=None, **kwargs):
        if not self.ready(connection, **kwargs):
            log(f'{self}: Not ready yet', 'debug')
            return
        log(f'{self}:', 'debug')
        df = kwargs.pop('data', None)
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
            await node.next(connection=connection, data = df.copy(), **kwargs)
        self.consume()
        return

class Intersection(FlowGraphNode):
    """Filter common columns from multiple dataframes of same length
    columnwise before passing to the next node.

    Performs an inner join on the dataframes and retains only those
    columns which are common.
    """
    def __init__(self, **kwargs):
        self.multi_input = True
        super().__init__(**kwargs)

    async def next(self, connection=None, **kwargs):
        if not self.ready(connection, **kwargs):
            log(f'{self}: Not ready yet', 'debug')
            return
        log(f'{self}:', 'debug')
        df = kwargs.pop('data', None)
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
            await node.next(connection=connection, data = df.copy(), **kwargs)
        self.consume()
        return

class ColumnFilter(FlowGraphNode):
    """Filter out columns from dataframe.

    Other than the columns mentioned in the map, drop the remainder.
    """
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
        df = kwargs.pop('data', None)
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
            await node.next(connection=connection, data = df.copy(), **kwargs)
        self.consume()
        return
