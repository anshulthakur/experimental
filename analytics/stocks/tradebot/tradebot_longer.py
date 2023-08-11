'''
This bot only goes long
'''
import asyncio

import sys, os

sys.path.append(os.path.join(os.path.dirname(__file__), '../'))
import init
from lib.logging import log, set_loglevel

from threading import Thread

from base import FlowGraph
from base.scheduler import AsyncScheduler as Scheduler
from nodes import DataFrameAggregator, Resampler, NseSource, FolderSource

from settings import project_dirs

from strategy.priceaction import EvolvingSupportResistance, Zigzag
from bots.examples import LongBot
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

    # create and start the new thread
    #thread = Broker(queue=queue)
    #thread.start()

    # Create a flowgraph
    fg = FlowGraph(name='FlowGraph', mode='backtest')

    # Add a dataframe source 
    #source = NseSource(name='NSE', symbol='NIFTY 50', timeframe='5min')
    source = FolderSource(name='NSE', 
                          symbol='NIFTY', 
                          timeframe='1H', 
                          folder=project_dirs.get('intraday'),
                          start_date='2022-01-01 09:15',
                          market_start_time='09:15:00',
                          offset=25)
    fg.add_node(source)
    #fg.add_node(source)

    res_sup_node = EvolvingSupportResistance(name="SuppRes", support_basis='low', resistance_basis='high')
    fg.add_node(res_sup_node)

    #TraderBot
    longbot = LongBot(name='LongBot', cash=20000000, lot_size=75)
    fg.add_node(longbot)
    fg.register_signal_handler([Resistance, Support, EndOfData, Shutdown], longbot)

    # Add some sink nodes 
    sink = DataFrameAggregator(name='Sink', filename='/tmp/ResistanceSupport.csv')
    fg.add_node(sink)
    fg.register_signal_handler([Resistance, Support, EndOfData], sink)
    
    #Add frequency scaling
    resampler = Resampler(interval=5*60, name='Resampler') #Running on a 5min scale
    fg.add_node(resampler)

    # connect the nodes together
    fg.connect(resampler, source)
    fg.connect(source, res_sup_node)
    fg.connect(res_sup_node, sink)
    fg.connect(res_sup_node, longbot)

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