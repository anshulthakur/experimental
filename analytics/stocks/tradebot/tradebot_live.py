import asyncio

import sys, os

sys.path.append(os.path.join(os.path.dirname(__file__), '../'))
from lib.logging import log, set_loglevel

from threading import Thread

from base import FlowGraph
from base.scheduler import AsyncScheduler as Scheduler
from nodes import DataFrameAggregator, Resampler, NseMultiStockSource

from strategy.priceaction import EvolvingSupportResistance
from bots.examples import LongBot
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
    source = NseMultiStockSource(name='NSE', timeframe='1min')
    fg.add_node(source)

    # Add some sink nodes 
    sink = DataFrameAggregator(name='Sink', filename='/tmp/ResistanceSupport.csv')
    fg.add_node(sink)
    fg.register_signal_handler([Resistance, Support, EndOfData], sink)
    
    #Add frequency scaling
    resampler = Resampler(interval=1*90, name='Resampler') #Running on a 1min 30s scale (rate of update of NSE website data)
    fg.add_node(resampler)

    # connect the nodes together
    fg.connect(resampler, source)
    fg.connect(source, sink)


    fg.display()
    # Create a scheduler
    scheduler = Scheduler(interval='1s', mode='stream') # 1 second scheduler

    # register some flowgraphs with the scheduler
    scheduler.register(fg)

    # start the scheduler
    await scheduler.run()
    scheduler.stop()
    #await asyncio.sleep(scheduler.interval)

    #thread.join()

if __name__ == "__main__":
    asyncio.run(main())#main()