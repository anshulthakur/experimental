'''
This bot only goes short
'''
import asyncio

import sys, os

sys.path.append(os.path.join(os.path.dirname(__file__), '../'))

from lib.logging import log, set_loglevel
from settings import project_dirs

from threading import Thread

from base import FlowGraph
from base.scheduler import AsyncScheduler as Scheduler
from nodes import DataFrameAggregator, Resampler, FolderSource, Indicator
from bots.examples import DynamicResistanceBot
from tradebot.base.signals import Resistance, Support, EndOfData

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

    # create and start the new thread
    #thread = Broker(queue=queue)
    #thread.start()

    # Create a flowgraph
    fg = FlowGraph(name='FlowGraph', mode='backtest')

    # Add a dataframe source 
    source = FolderSource(name='NSE', 
                          symbol='NIFTY', 
                          timeframe='1H', 
                          folder=project_dirs.get('intraday'),
                          start_date='2022-01-01 09:15',
                          market_start_time='09:15:00',
                          offset=25)
    fg.add_node(source)

    # Add indicator nodes
    node_indicators = Indicator(name='Indicators', transparent=True, indicators=[{'tagname': 'EMA20', 
                                                                'type': 'EMA', 
                                                                'length': 20,
                                                                'column': 'close'},
                                                               ])
    fg.add_node(node_indicators)

    #TraderBot
    shortbot = DynamicResistanceBot(name='ShortBot', 
                                    value=20, 
                                    proximity=1.0, 
                                    cash=20000000, 
                                    lot_size=75,
                                    overnight_positions=False,
                                    last_candle_time='15:15:00')
    fg.add_node(shortbot)
    fg.register_signal_handler([EndOfData], shortbot)

    # Add some sink nodes 
    sink = DataFrameAggregator(name='Sink', filename='/tmp/ResistanceSupport.csv')
    fg.add_node(sink)
    fg.register_signal_handler([Resistance, Support, EndOfData], sink)
    
    #Add frequency scaling
    resampler = Resampler(interval=1, name='Resampler') #Running on a 15min scale
    fg.add_node(resampler)

    # connect the nodes together
    fg.connect(resampler, source)
    fg.connect(source, node_indicators)
    fg.connect(node_indicators, sink)
    fg.connect(node_indicators, shortbot)

    fg.display()
    # Create a scheduler
    scheduler = Scheduler(interval=1, mode='backtest') # 1 second scheduler

    # register some flowgraphs with the scheduler
    scheduler.register(fg)

    # start the scheduler
    await scheduler.run()
    scheduler.stop()
    #await asyncio.sleep(scheduler.interval)

    #thread.join()

if __name__ == "__main__":
    asyncio.run(main())#main()