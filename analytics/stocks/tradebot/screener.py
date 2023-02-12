import asyncio

import sys, os

sys.path.append(os.path.join(os.path.dirname(__file__), '../'))

import django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'settings')
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from lib.logging import log, set_loglevel

from threading import Thread

from base import FlowGraph
from base.scheduler import AsyncScheduler as Scheduler
from nodes import Sink, Resampler, NseMultiStockSource, Indicator, DataFrameSink, TradingViewSource, ColumnFilter, MultiStockSource
from strategy.screen import EMA_RSI_Screen, Proximity_Screen, Crossover_Screen, EMA_Filter, RSI_Filter

from tradebot.base.signals import EndOfData

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
    #source = TradingViewSource(name='Stock', symbol='KABRAEXTRU', exchange='NSE', timeframe='1d')
    #source = NseMultiStockSource(name='Source', exchange='NSE', timeframe='1d', offline=True, offset=200)
    source = MultiStockSource(name='Source', timeframe='1d', offline=True, offset=200, member_file='portfolio.json')
    fg.add_node(source)

    #Add a column filter node
    #filterNode = ColumnFilter(name='Formatter', map = {'close': 'close'})
    #fg.add_node(filterNode)

    # Add indicator nodes
    node_indicators = Indicator(name='Indicators', indicators=[{'tagname': 'EMA20', 
                                                                'type': 'EMA', 
                                                                'length': 20,
                                                                'column': 'close'},
                                                               {'tagname': 'EMA200', 
                                                                'type': 'EMA', 
                                                                'length': 200,
                                                                'column': 'close'},
                                                               {'tagname': 'RSI', 
                                                                'type': 'RSI', 
                                                                'length': 14,
                                                                'column': 'close'}
                                                               ])
    fg.add_node(node_indicators)

    # Add screener node
    screener = EMA_RSI_Screen(name="EMA-RSI-Screen")
    fg.add_node(screener)

    # Add proximity node
    proximity = Proximity_Screen(name="Proximity-Scanner", what='close', near='EMA20', by=0.01, direction='up', filters=[EMA_Filter(value=200)])
    fg.add_node(proximity)

    # Add crossover node
    cross = Crossover_Screen(name="Crossover-Scanner", what='close', crosses='EMA20', direction='up', filters=[EMA_Filter(value=200)])
    fg.add_node(cross)

    # Add some sink nodes 
    sink = Sink(name='Sink')
    fg.add_node(sink)
    fg.register_signal_handler([EndOfData], sink)

    proxy_sink = Sink(name='Proximity')
    fg.add_node(proxy_sink)
    fg.register_signal_handler([EndOfData], proxy_sink)

    cross_sink = Sink(name='Crossovers')
    fg.add_node(cross_sink)
    fg.register_signal_handler([EndOfData], cross_sink)

    df_sink = DataFrameSink(name='DF-Sink')
    fg.add_node(df_sink)
    fg.register_signal_handler([EndOfData], df_sink)

    #Add frequency scaling
    resampler = Resampler(interval=1, name='Resampler') #Running on a 1min 30s scale (rate of update of NSE website data)
    fg.add_node(resampler)

    # connect the nodes together
    fg.connect(resampler, source)
    #fg.connect(source, filterNode)
    #fg.connect(filterNode, node_indicators)
    fg.connect(source, node_indicators)
    fg.connect(node_indicators, screener)
    fg.connect(screener, sink)

    fg.connect(node_indicators, proximity)
    fg.connect(proximity, proxy_sink)

    fg.connect(node_indicators, cross)
    fg.connect(cross, cross_sink)

    fg.connect(node_indicators, df_sink)

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