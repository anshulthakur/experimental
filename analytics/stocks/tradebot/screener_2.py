import asyncio

import sys, os

sys.path.append(os.path.join(os.path.dirname(__file__), '../'))

import init

from lib.logging import log, set_loglevel

from base import FlowGraph
from base.scheduler import AsyncScheduler as Scheduler
from nodes import Sink, Resampler, NseMultiStockSource, Indicator, DataFrameSink, TradingViewSource, ColumnFilter, MultiStockSource
from strategy.screen import EMA_RSI_Screen, Proximity_Screen, Crossover_Screen, EMA_Filter, Price_Filter, CustomScreen, Eval_Filter

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
    source = MultiStockSource(name='Source', timeframe='1M', offline=False, offset=-1, min_entries=6, 
                              member_file='nselist.json')
    fg.add_node(source)

    # Add indicator nodes
    node_indicators = Indicator(name='Indicators', indicators=[{'tagname': 'EMA5', 
                                                                'type': 'EMA', 
                                                                'length': 5,
                                                                'column': 'close'},
                                                               ])
    fg.add_node(node_indicators)

    # Add screener node
    screener = CustomScreen(name="Screener", filters=[Price_Filter(level=-2, key='close', condition='near', margin='0.01'),
                                                      Eval_Filter(condition="x.iloc[-2]['close'] <= x.iloc[-3]['close']")])
    fg.add_node(screener)

    # Add some sink nodes 
    sink = Sink(name='Sink')
    fg.add_node(sink)
    fg.register_signal_handler([EndOfData,Shutdown], sink)

    df_sink = DataFrameSink(name='DF-Sink')
    fg.add_node(df_sink)
    fg.register_signal_handler([EndOfData,Shutdown], df_sink)

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