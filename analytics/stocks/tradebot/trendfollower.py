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
from nodes import Sink, Resampler, NseMultiStockSource, Indicator, DataFrameSink, TradingViewSource, ColumnFilter
from nodes import MinMaxDetector
from strategy.priceaction import Zigzag
from bots.examples import  LongBot

from tradebot.base.signals import Resistance, Support, EndOfData, Shutdown

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
    source = TradingViewSource(name='Stock', symbol='KABRAEXTRU', exchange='NSE', timeframe='1d')
    #source = NseMultiStockSource(name='Source', exchange='NSE', timeframe='15m', offline=False, offset=200, min_entries=400,)
    fg.add_node(source)

    #Drop columns except close
    filterNode = ColumnFilter(name='ColumnFilter', map = {'close': 'close'})
    fg.add_node(filterNode)

    # Add indicator nodes
    maxmin = MinMaxDetector(name='MaxMin', lookaround=1)
    fg.add_node(maxmin)

    # Add trend indicator node
    trend = Zigzag(name='TrendDetector')
    fg.add_node(trend)

    #TraderBot
    longbot = LongBot(name='LongBot', cash=20000000, lot_size=100)
    fg.add_node(longbot)
    fg.register_signal_handler([Resistance, Support, EndOfData, Shutdown], longbot)

    # Add some sink nodes 
    sink = Sink(name='Sink')
    fg.add_node(sink)
    fg.register_signal_handler([EndOfData], sink)

    df_sink = DataFrameSink(name='DF-Sink')
    fg.add_node(df_sink)
    fg.register_signal_handler([EndOfData], df_sink)

    #Add frequency scaling
    resampler = Resampler(interval=1, name='Resampler') #Running on a 1min 30s scale (rate of update of NSE website data)
    fg.add_node(resampler)

    # connect the nodes together
    fg.connect(resampler, source)
    fg.connect(source, filterNode)
    #fg.connect(filterNode, node_indicators)
    fg.connect(filterNode, maxmin)
    fg.connect(maxmin, trend)
    fg.connect(filterNode, trend)
    fg.connect(trend, sink)
    fg.connect(filterNode, longbot)
    fg.connect(longbot, df_sink)

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