"""
The bot checks if some alerts have been triggered
"""
import asyncio

import sys, os

sys.path.append(os.path.join(os.path.dirname(__file__), '../'))

# import django
# os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'settings')
# os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
# django.setup()
import init

from lib.logging import log, set_loglevel

from base import FlowGraph
from base.scheduler import AsyncScheduler as Scheduler
from nodes import Sink, Resampler, Indicator, DataFrameSink, MultiStockSource
from nodes.studies import PriceAlerts

from tradebot.base.signals import EndOfData, Shutdown

import signal, os

def signal_handler(signum, frame):
    signame = signal.Signals(signum).name
    print(f'Signal handler called with signal {signame} ({signum})')
    global scheduler
    scheduler.stop()

async def main():
    global scheduler
    set_loglevel('debug')
    # Set the signal handler and a 5-second alarm
    signal.signal(signal.SIGINT, signal_handler)

    # Create a flowgraph
    fg = FlowGraph(name='FlowGraph', mode='backtest')

    # Add a dataframe source 
    watchlist_file = 'prospect.json'
    source = MultiStockSource(name='Source', timeframe='1d', offline=True, offset=200, min_entries=400, member_file=watchlist_file)
    fg.add_node(source)

    #Add a column filter node
    #filterNode = ColumnFilter(name='Formatter', map = {'close': 'close'})
    #fg.add_node(filterNode)

    # Add indicator nodes
    node_indicators = Indicator(name='Indicators', indicators=[
                                                                {'tagname': 'EMA20', 
                                                                'type': 'EMA', 
                                                                'length': 20,
                                                                'column': 'close'},
                                                               {'tagname': 'EMA200', 
                                                                'type': 'EMA', 
                                                                'length': 200,
                                                                'column': 'close'},
                                                               ])
    fg.add_node(node_indicators)

    # Add the alerting node
    watcher = PriceAlerts(name='Alerts', file=f'./runtime/lists/{watchlist_file}', timeframe='1D')
    fg.add_node(watcher)

    # Add some sink nodes
    df_sink = DataFrameSink(name='DF-Sink', print_logs = False)
    fg.add_node(df_sink)
    fg.register_signal_handler([EndOfData,Shutdown], df_sink)

    #Add frequency scaling
    resampler = Resampler(interval=1, name='Resampler') #Running on a 1min 30s scale (rate of update of NSE website data)
    fg.add_node(resampler)

    # connect the nodes together
    fg.connect(resampler, source)
    fg.connect(source, node_indicators)
    fg.connect(node_indicators, watcher)
    fg.connect(watcher, df_sink)

    fg.display()
    # Create a scheduler
    scheduler = Scheduler(interval='1s', mode='backtest') # 1 second scheduler

    # register some flowgraphs with the scheduler
    scheduler.register(fg)

    # start the scheduler
    await scheduler.run()
    scheduler.stop()
    #await asyncio.sleep(scheduler.interval)

    #thread.join()

if __name__ == "__main__":
    asyncio.run(main())#main()