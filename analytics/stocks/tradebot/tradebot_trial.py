import asyncio
import aiohttp
import json
from lib.logging import log, set_loglevel

import time
import threading

from base import FlowGraph
from base.scheduler import AsyncScheduler as Scheduler
from nodes import IndicatorNode

# define an async callback function to be registered with a node
async def signal_callback(signal_data, emitting_node):
    # do some asynchronous processing here
    await asyncio.sleep(1)
    print(f"Received signal {signal_data} from node {emitting_node}")



# create a queue for communication between the scheduler and the new thread
queue = asyncio.Queue()
# define the function for the new thread
def thread_function():
    while True:
        # wait for a request to be received on the queue
        request = queue.get()
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


async def test_run():
    # create and start the new thread
    thread = threading.Thread(target=thread_function)
    thread.start()

    # Create a flowgraph
    fg = FlowGraph()

    # add some nodes to the flowgraph
    node1 = IndicatorNode(indicators=[{'tagname': 'RSI', 'indicator': 'RSI', 'length': 14}])
    node2 = IndicatorNode(indicators=[{'tagname': 'EMA10', 'indicator': 'EMA', 'length': 10},
                                      {'tagname': 'EMA20', 'indicator': 'EMA', 'length': 20}
                                     ])
    fg.add_node(node1)
    fg.add_node(node2)

    # connect the nodes
    fg.connect(node1, node2)

    # run the flowgraph
    fg.run()

    # display the flowgraph
    fg.display()

    async_scheduler = Scheduler(5)
    # register some flowgraphs with the scheduler
    fg1 = FlowGraph()
    fg2 = FlowGraph()
    await async_scheduler.register(fg1)
    await async_scheduler.register(fg2)

    # start the scheduler
    while True:
        # send a request to the new thread
        queue.put_nowait("some_request")
        # wait for a response from the new thread
        response = queue.get()
        print(response)
        await async_scheduler.run()
        await asyncio.sleep(async_scheduler.interval)

class Broker(object):
    pass

class Bot(object):
    pass 

class Strategy(object):
    pass


def main():
    set_loglevel('debug')
    #Create a scheduler
    scheduler = Scheduler()

    #Create a separate thread for Broker communication
    pass


if __name__ == "__main__":
    main()