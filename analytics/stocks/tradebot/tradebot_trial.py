import asyncio
import aiohttp
import json
import sys, os

sys.path.append(os.path.join(os.path.dirname(__file__), '../'))
from lib.logging import log, set_loglevel

import time
import threading
from threading import Thread

from base import FlowGraph
from base.scheduler import AsyncScheduler as Scheduler
from nodes import IndicatorNode, NseSource, FileSink, Resampler

import signal, os

def signal_handler(signum, frame):
    signame = signal.Signals(signum).name
    print(f'Signal handler called with signal {signame} ({signum})')
    global scheduler
    scheduler.stop()

# define an async callback function to be registered with a node
async def signal_callback(signal_data, emitting_node):
    # do some asynchronous processing here
    await asyncio.sleep(1)
    # send a request to the new thread
    queue.put_nowait("some_request")
    # wait for a response from the new thread
    response = queue.get()
    print(response)
    print(f"Received signal {signal_data} from node {emitting_node}")

# The broker is implemented as a separate thread for communicating with the broker endpoint
class Broker(Thread):
    def __init__(self, queue):
        self.queue = queue
        pass

    def run(self):
        while True:
            # wait for a request to be received on the queue
            request = self.queue.get()
            if request == "some_request":
                # process the request and send a response on the queue
                response = "some_response"
                queue.put_nowait(response)

# define an async callback function to execute trades
async def execute_trade(signal_data, emitting_node):
    # extract the trade details from the signal data
    symbol, action, quantity = signal_data
    # execute the trade using an API or other interface
    await place_trade(symbol, action, quantity)

# define an async function to place a trade using an API or other interface
async def place_trade(symbol, action, quantity):
    pass  # implement the trade execution logic here

# define an async callback function to process data and generate trade signals
async def process_data(data, emitting_node):
    # extract relevant information from the data
    symbol = data["symbol"]
    price = data["price"]
    volume = data["volume"]
    # use the data to generate trade signals
    if price > 50 and volume > 100:
        signal_data = (symbol, "buy", 100)
        await emitting_node.emit_signal(signal_data)
    elif price < 30 and volume < 50:
        signal_data = (symbol, "sell", 50)
        await emitting_node.emit_signal(signal_data)

# define an async callback function to subscribe to a stock market feed
async def subscribe_to_feed(emitting_node):
    async with aiohttp.ClientSession() as session:
        while True:
            async with session.get("https://example.com/stock-data") as response:
                data = await response.json()
                await emitting_node.emit_signal(data)
            await asyncio.sleep(1)


# create a queue for communication between the scheduler and the new thread
queue = asyncio.Queue()
running = False

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
    source = NseSource(name='NSE', symbol='NIFTY 50', timeframe='5min')
    fg.add_node(source)

    # Add some indicator nodes to the flowgraph
    node1 = IndicatorNode(name='RSI', indicators=[{'tagname': 'RSI', 
                                                    'type': 'RSI', 
                                                    'length': 14,
                                                    'column': 'close'}])
    node2 = IndicatorNode(name='EMA', indicators=[{'tagname': 'EMA10', 
                                                    'type': 'EMA', 
                                                    'length': 10,
                                                    'column': 'close'},
                                                  {'tagname': 'EMA20', 
                                                    'type': 'EMA', 
                                                    'length': 20,
                                                    'column': 'close'}
                                                ])
    fg.add_node(node1)
    fg.add_node(node2)

    # Add some sink nodes 
    #sink1 = FileSink(name='RSIDump', filename='/tmp/RsiDump.csv')
    #sink2 = FileSink(name='EMADump', filename='/tmp/EmaDump.csv')
    #fg.add_node(sink1)
    #fg.add_node(sink2)
    sink3 = FileSink(name='CombinedDump', filename='/tmp/CombinedDump.csv')
    fg.add_node(sink3)
    
    #Add frequency scaling
    resampler = Resampler(interval=5*60, name='Resampler') #Running on a 5min scale
    fg.add_node(resampler)

    # connect the nodes together
    fg.connect(source, node1)
    fg.connect(source, node2)
    #fg.connect(node1, sink1)
    fg.connect(node1, sink3)
    #fg.connect(node2, sink2)
    fg.connect(node2, sink3)
    fg.connect(resampler, source)

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