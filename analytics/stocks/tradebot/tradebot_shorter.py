'''
This bot only goes short
'''
import asyncio

import sys, os

sys.path.append(os.path.join(os.path.dirname(__file__), '../'))
import init
from lib.logging import log, set_loglevel
from settings import project_dirs

from threading import Thread

from base import FlowGraph
from base.scheduler import AsyncScheduler as Scheduler
from nodes import DataFrameAggregator, Resampler, FolderSource, Indicator, DataResampler
from bots.examples import DynamicResistanceBot
from tradebot.base.signals import Resistance, Support, EndOfData, Shutdown, EndOfDay

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

    tf_value=60
    sl_tf_value = 5
    tf_unit = 'm'
    timeframe = f'{tf_value}{tf_unit}'
    sl_timeframe = f'{sl_tf_value}{tf_unit}'

    #Add frequency scaling
    resampler = Resampler(interval=1, name='Main Resampler') #Running on a 15min scale
    fg.add_node(resampler)

    # Add a dataframe source 
    source = FolderSource(name='NIFTY', 
                          symbol='NIFTY', 
                          timeframe='1Min', 
                          folder=project_dirs.get('intraday'),
                          start_date='2022-01-01 09:15',
                          market_start_time='09:15:00',
                          offset=0)
    fg.add_node(source)

    #Add data resampling
    data_resampler = DataResampler(name='Data Resampler', interval=tf_value)
    fg.add_node(data_resampler)

    data_resampler2 = DataResampler(name='Data Resampler 1', interval=sl_tf_value, publications=[sl_timeframe])
    fg.add_node(data_resampler2)

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
                                    last_candle_time='15:15:00',
                                    timeframe=timeframe,
                                    stop_loss_tf=sl_timeframe)
    fg.add_node(shortbot)
    fg.register_signal_handler([EndOfData, Shutdown, EndOfDay], shortbot)

    # Add some sink nodes 
    sink = DataFrameAggregator(name='Sink', filename='/tmp/ResistanceSupport.csv')
    fg.add_node(sink)
    fg.register_signal_handler([Resistance, Support, EndOfData], sink)
    
    null_sink = DataFrameAggregator(name='NullSink', )
    fg.add_node(null_sink)

    # connect the nodes together
    fg.connect(resampler, source)
    fg.connect(source, data_resampler)
    fg.connect(data_resampler, node_indicators)
    fg.connect(node_indicators, sink)
    fg.connect(node_indicators, shortbot)

    fg.connect(source, data_resampler2)
    fg.connect(data_resampler2, null_sink)

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