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
from nodes import Sink, Resampler, NseMultiStockSource, Indicator, DataFrameSink, TradingViewSource, ColumnFilter, MultiStockSource
from strategy.screen import EMA_RSI_Screen, Proximity_Screen, Crossover_Screen, EMA_Filter, RSI_Filter, CustomScreen

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
    #source = TradingViewSource(name='Stock', symbol='KABRAEXTRU', exchange='NSE', timeframe='1d')
    #source = NseMultiStockSource(name='Source', exchange='NSE', timeframe='1d', offline=True, offset=200)
    #source = MultiStockSource(name='Source', timeframe='1d', offline=True, offset=200, member_file='portfolio.json')
    source = MultiStockSource(name='Source', timeframe='1d', offline=True, offset=-1, min_entries=400, member_file='universe.json')
    fg.add_node(source)

    source_w = MultiStockSource(name='Weekly_Source', timeframe='1M', offline=True, offset=-1, min_entries=40, member_file='universe.json')
    fg.add_node(source_w)

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

    node_indicators_2 = Indicator(name='Indicators2', indicators=[{'tagname': 'EMA20', 
                                                                'type': 'EMA', 
                                                                'length': 20,
                                                                'column': 'close'},
                                                               {'tagname': 'RSI', 
                                                                'type': 'RSI', 
                                                                'length': 14,
                                                                'column': 'close'}
                                                               ])
    fg.add_node(node_indicators_2)

    # Add screener node
    screener = EMA_RSI_Screen(name="EMA-RSI-Screen")
    fg.add_node(screener)

    rsi_screen = CustomScreen(name="DeepRSI", filters=[RSI_Filter(value=35, greater=False)])
    fg.add_node(rsi_screen)

    # Add proximity node
    proximity_up = Proximity_Screen(name="Upside-Proximity-Scanner", what='close', near='EMA20', by=0.01, direction='up', filters=[EMA_Filter(value=200)])
    fg.add_node(proximity_up)

    proximity_down = Proximity_Screen(name="Downside-Proximity-Scanner", what='close', near='EMA20', by=0.01, direction='down', filters=[EMA_Filter(value=200)])
    fg.add_node(proximity_down)

    # Add crossover node
    cross_up = Crossover_Screen(name="Upside-Crossover-Scanner", what='close', crosses='EMA20', direction='up', filters=[EMA_Filter(value=200)])
    fg.add_node(cross_up)

    cross_down = Crossover_Screen(name="Downside-Crossover-Scanner", what='close', crosses='EMA20', direction='down', filters=[EMA_Filter(value=200)])
    fg.add_node(cross_down)

    # Add some sink nodes 
    sink = Sink(name='Sink')
    fg.add_node(sink)
    fg.register_signal_handler([EndOfData,Shutdown], sink)

    proxy_sink = Sink(name='Proximity-Upside')
    fg.add_node(proxy_sink)
    fg.register_signal_handler([EndOfData,Shutdown], proxy_sink)

    proxy_sink_2 = Sink(name='Proximity-Downside')
    fg.add_node(proxy_sink_2)
    fg.register_signal_handler([EndOfData,Shutdown], proxy_sink_2)

    cross_sink = Sink(name='Upside-Crossovers')
    fg.add_node(cross_sink)
    fg.register_signal_handler([EndOfData,Shutdown], cross_sink)

    cross_sink_2 = Sink(name='Downside-Crossovers')
    fg.add_node(cross_sink_2)
    fg.register_signal_handler([EndOfData,Shutdown], cross_sink_2)

    deeprsi_sink = Sink(name='Deep-RSI')
    fg.add_node(deeprsi_sink)
    fg.register_signal_handler([EndOfData,Shutdown], deeprsi_sink)

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
    fg.connect(source_w, node_indicators_2)
    fg.connect(node_indicators, screener)
    fg.connect(node_indicators_2, rsi_screen)
    fg.connect(screener, sink)
    fg.connect(rsi_screen, deeprsi_sink)

    fg.connect(node_indicators, proximity_up)
    fg.connect(node_indicators, proximity_down)
    fg.connect(proximity_up, proxy_sink)
    fg.connect(proximity_down, proxy_sink_2)

    fg.connect(node_indicators, cross_up)
    fg.connect(cross_up, cross_sink)

    fg.connect(node_indicators, cross_down)
    fg.connect(cross_down, cross_sink_2)

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