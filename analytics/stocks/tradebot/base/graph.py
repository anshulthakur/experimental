from lib.logging import log
from .base import BaseClass
from .signals import Shutdown
import copy
import time

class FlowGraphNode(BaseClass):
    '''
    A FlowGraphNode is the basic unit of a flowgraph. It represents the vertex in a directed graph.
    The basic flow of signals is from one node to all the nodes downstream to it through 
    its output ports, if applicable.

    Overall, a FlowgraphNode must be connected to some other nodes for it to work. If the
    node doesn't have any upstream connection (or input ports), it is the root node. This means that it should
    be the one originating a signal which flows downstream.

    Any other node keeps track of its upstream connections as well as downstream ones. Once it 
    has received a signal input on all its input ports, the node invokes the processing of its
    'next' function, where it works on the incoming data. If it has some data to send downstream,
    it iterates on its output ports and places a copy of its generated data on each port. Finally, 
    it must clear it's input ports by calling 'consume' method. In case there
    is no data to send forward, it simply returns.

    A node may also generate signals by invoking 'emit', which may be emitted asynchronously such that all the nodes
    registering to receive those signals will receive a copy, irrespective of whether they are
    connected to this node or not. It must first inform the Flowgraph about its capability about the signals 
    during initialization that it can generate.
    '''
    def __init__(self, name=None, strict = True, connections=[], signals=[], publications=[], timeframe=None, **kwargs):
        #print(kwargs)
        self.connections = copy.deepcopy(connections)
        self._flowgraph = None
        self.is_root = True
        self.name = name
        self.signals = signals
        self.callbacks = {}
        for signal in self.signals:
            self.callbacks[signal.name()] = []
        self.streams = {}
        for publication in publications:
            self.streams[self.sanitize_timeframe(publication)] = []
        self.multi_input = False #By default, single input
        self.inputs = {}
        if name is None:
            raise Exception('name must be provided')
        self.mode = None
        self.input_types = None
        self.strict = strict
        self.wait_for_all = False #Useful when the node is stateless and can support multiple input connections.
        self.timeframe = self.sanitize_timeframe(timeframe) if timeframe is not None else None
        #On multi-timeframe analysis, a smaller time frame may trigger an event while we're still awaiting the processing to complete on a larger TF.
        #We avoid this by flagging busy sections and making the other processing to wait until we're out.
        self.busy = False 
        self.pending_subscriptions = []
        super().__init__(**kwargs)

    def get_connection_name(self, node, tag=None):
        if tag is not None:
            return str(tag)
        name = f"{self.name.replace(' ','_')}_{node.name.replace(' ','_')}"
        return name

    @property
    def flowgraph(self):
        return self._flowgraph

    @flowgraph.setter
    def flowgraph(self, flowgraph):
        self._flowgraph = flowgraph

    def connect(self, node, tag=None):
        log(f'Connect {node} to {self}', 'debug')
        self.connections.append((node, self.get_connection_name(node, tag)))
        if tag is None:
            node.add_input(f"{self.get_connection_name(node, tag)}")
        else:
            node.add_input(tag)

    def add_input(self, name):
        if not self.multi_input and len(self.inputs)==1:
            raise Exception(f"Cannot add more than one connection to {self.name}")
        if name in self.inputs:
            raise Exception(f"Input for {name} already exists in {self.name}")
        self.inputs[name] = None

    async def emit(self, signal):
        if signal.name() not in self.callbacks:
            raise Exception(f'Unsupported emit for signal {signal.name()}')
        for callback in self.callbacks[signal.name()]:
            await callback(signal)

    def register(self, signal, callback):
        if signal.name() not in self.callbacks:
            raise Exception(f"{self} does not support signal {signal.name()}")
        self.callbacks[signal.name()].append(callback)

    def register_subscriber(self, event, callback):
        if event.timeframe in self.streams:
            self.streams[event.timeframe].append((event, callback))
            log(f'{self.name} Added subscriber for event {event.name}', 'debug')

    def unregister_subscriber(self, event):
        if event.timeframe in self.streams:
            for ii in range(0, len(self.streams[event.timeframe])):
                if self.streams[event.timeframe][ii][0].name == event.name:
                    self.streams[event.timeframe].pop(ii)
                    #log(f'{self.name} Unsubscribed node for event {event.name}', 'debug')
                    break

    async def notify(self, df):
        for stream in self.streams:
            for (event, callback) in self.streams[stream]:
                # if event.active:
                #     log(f'Test {event}', 'debug')
                if event.active and event.trigger(df):
                    await callback(copy.deepcopy(event))
                    if not event.recurring:
                        event.active = False
    
    async def handle_signal(self, signal):
        log(f"{self.name}: Received signal {signal.name()}.", 'debug')
        return

    def subscribe(self, event):
        event.subscriber = self.name
        if self.flowgraph is not None:
            self.flowgraph.subscribe(event=event, callback=self.handle_event_notification)
        else:
            self.pending_subscriptions.append(event)

    def unsubscribe(self, event):
        self.flowgraph.unsubscribe(event=event)

    async def handle_event_notification(self, event):
        log(f'Event {event.name} received.', 'debug')
        log(f'{event.df}', 'debug')
        if not event.recurring:
            self.unsubscribe(event)
        return

    def display_connections(self, offset=0):
        disp = ''
        parent_offset = offset
        for connection,name in self.connections:
            if self.is_root:
                disp = f"\n[{self}]"
                print(disp, end=" ")
                parent_offset += len(disp)
            disp = f"+-({name})--> [{connection}]"
            print(disp, end=" ")
            connection.display_connections(offset = parent_offset+len(disp))
            print(f"\n{' '*parent_offset}", end=' ')

    def ready(self, connection=None, **kwargs):
        if self.strict and self.input_types is not None and self.input_types!=type(kwargs.get('data')).__name__:
            raise Exception(f"Input types across various inputs must be consistent. Excepted: {self.input_types}, received: {type(kwargs.get('data')).__name__}")
        self.input_types = type(kwargs.get('data')).__name__
        if self.is_root:
            return True
        if connection is not None:
            #log(f'{self.inputs}')
            self.inputs[connection] = kwargs.get('data')
        elif connection is None and self.multi_input is False:
            for connection in self.inputs: #Should iterate only once
                self.inputs[connection] = kwargs.get('data')
        else:
            raise Exception('Non-root node must have connection name explicitly passed')
        if self.wait_for_all is True:
            for connections in self.inputs:
                if self.inputs[connections] is None:
                    #Not all inputs are received yet
                    return False
        return True
    
    def wait_until_busy(self):
        sleep_duration = 1/100000.0
        while self.busy:
            time.sleep(sleep_duration)

    def consume(self):
        for connections in self.inputs:
            if self.inputs[connections] is not None:
                self.inputs[connections] = None

    async def next(self, connection=None, **kwargs):
        pass

    def __str__(self):
        return self.name

class FlowGraph(BaseClass):
    def __init__(self, name=None, frequency=None, mode='buffered'):
        self.name = name
        self._frequency = frequency
        self.node_names = []
        self.nodes = []
        self.roots = []
        self.signals = {}
        self.events = {}
        self.mode = mode
        self.sighandler = FlowGraphNode(name='SigHandler', signals=[Shutdown])
        if self.mode not in ['stream', 'buffered', 'backtest']:
            log(f'Unrecognized mode "{self.mode}".', 'error')
            raise Exception(f'Unrecognized mode "{self.mode}".')

    @property
    def frequency(self):
        return self._frequency

    @frequency.setter
    def frequency(self, frequency):
        if frequency is not None:
            try:
                self._frequency = float(frequency)
            except:
                log(f"Error setting frequency to {frequency}. Ensure that floats are passed", "error")
                pass

    def add_node(self, node):
        if hasattr(node, 'flowgraph'):
            node.flowgraph = self
            for subscription in node.pending_subscriptions:
                node.flowgraph.subscribe(subscription, node.handle_event_notification)
            node.pending_subscriptions = []
        else:
            log(f"Node doesn't have flowgraph attribute. {type(node)}", 'error')
            raise Exception('Class of node must be FlowGraphNode or inherited from it')
        if node.name in self.node_names:
            raise Exception('Node with same name already exists')
        self.nodes.append(node)
        self.roots.append(node)
        node.mode = self.mode
        for signal in node.signals:
            if signal.name() in self.signals:
                self.signals[signal.name()].append(node)
            else:
                self.signals[signal.name()] = [node]
        self.node_names.append(node.name)
        for stream in node.streams:
            if stream not in self.events:
                self.events[stream] = {'publishers': [node],
                                        }
            else:
                self.events[stream]['publishers'].append(node)
                for event in self.events[stream]:
                    if event != 'publishers':
                        node.register_subscriber(event = self.events[stream][event][0], callback=self.events[stream][event][1])

        log(f'Added {node} to flowgraph {self}', 'debug')

    def connect(self, from_node, to_node, tag=None):
        if from_node not in self.nodes:
            log(f'{from_node} not added to flowgraph {self}', 'error')
            raise Exception(f'{from_node} not added to flowgraph {self}')
        if to_node not in self.nodes:
            log(f'{to_node} not added to flowgraph {self}', 'error')
            raise Exception(f'{to_node} not added to flowgraph {self}')

        from_node.connect(to_node, tag)
        if to_node in self.roots:
            log(f'Remove {to_node} from root list', 'debug')
            self.roots.remove(to_node)
            to_node.is_root = False

    async def run(self, **kwargs):
        # code for running the flowgraph goes here
        # Start from the first node in the flowgraph and run to completion
        for node in self.roots:
            await node.next(connection=None, **kwargs)

    def display(self):
        # code for displaying the flowgraph goes here
        for node in self.roots:
            node.display_connections()
        print('\n')

    def subscribe(self, event, callback):
        #Make an entry in the flowgraph events dictionary regarding the subscription
        if event.timeframe not in self.events:
            self.events[event.timeframe] = {'publishers': []
                                            }
        if event.name in ['publishers']:
            raise Exception(f'Cannot use this name {event.name} for event')
        if event.name not in self.events[event.timeframe]:
            self.events[event.timeframe][event.name] = (event, callback)
        else:
            print(self.events[event.timeframe][event.name])
            raise Exception(f'Event with name {event.name} already exists.')
        #For nodes that already exist as publishers, subscribe to their publications
        for publisher in self.events[event.timeframe]['publishers']:
            publisher.register_subscriber(event, callback)
        pass

    def unsubscribe(self, event):
        if event.timeframe not in self.events:
            return
        if event.name in self.events[event.timeframe]:
            del(self.events[event.timeframe][event.name])
            #log(f'{self.events[event.timeframe]}')
        for publisher in self.events[event.timeframe]['publishers']:
            publisher.unregister_subscriber(event)

    def register_signal_handler(self, signals, node):
        if node in self.nodes:
            for signal in signals:
                if signal.name() in self.signals:
                    for s in self.signals[signal.name()]:
                        s.register(signal, node.handle_signal)
                if signal.name() == Shutdown.name():
                    self.sighandler.register(signal, node.handle_signal)
        else:
            raise ValueError("Node is not in flowgraph")

    def __str__(self):
        return self.name