from lib.logging import log
import copy
class BaseClass(object):
    def __init__(self, **kwargs):
        super().__init__()

class FlowGraphNode(BaseClass):
    def __init__(self, name=None, strict = True, connections=[], signals=[], **kwargs):
        #print(kwargs)
        self.connections = copy.deepcopy(connections)
        self._flowgraph = None
        self.is_root = True
        self.name = name
        self.signals = signals
        self.callbacks = {}
        for signal in self.signals:
            self.callbacks[signal.name()] = []
        self.multi_input = False #By default, single input
        self.inputs = {}
        if name is None:
            raise Exception('name must be provided')
        self.mode = None
        self.input_types = None
        self.strict = strict
        super().__init__(**kwargs)

    def get_connection_name(self, node):
        name = f"{self.name.replace(' ','_')}_{node.name.replace(' ','_')}"
        return name

    @property
    def flowgraph(self):
        return self._flowgraph

    @flowgraph.setter
    def flowgraph(self, flowgraph):
        self._flowgraph = flowgraph

    def connect(self, node):
        log(f'Connect {node} to {self}', 'debug')
        self.connections.append((node, self.get_connection_name(node)))
        node.add_input(f"{self.name}_{node.name}")

    def add_input(self, name):
        if not self.multi_input and len(self.inputs)==1:
            raise Exception(f"Cannot add more than one connection to {self.name}")
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

    async def handle_signal(self, signal):
        log(f"Received signal {signal.name()}.", 'debug')
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
            self.inputs[connection] = kwargs.get('data')
        elif connection is None and self.multi_input is False:
            for connection in self.inputs: #Should iterate only once
                self.inputs[connection] = kwargs.get('data')
        else:
            raise Exception('Non-root node must have connection name explicitly passed')
        for connections in self.inputs:
            if self.inputs[connections] is None:
                #Not all inputs are received yet
                return False
        return True

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
        self.nodes = []
        self.roots = []
        self.signals = {}
        self.mode = mode
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
        else:
            log(f"Node doesn't have flowgraph attribute. {type(node)}", 'error')
            raise Exception('Class of node must be FlowGraphNode or inherited from it')
        self.nodes.append(node)
        self.roots.append(node)
        node.mode = self.mode
        for signal in node.signals:
            if signal.name() in self.signals:
                self.signals[signal.name()].append(node)
            else:
                self.signals[signal.name()] = [node]
        log(f'Added {node} to flowgraph {self}', 'debug')

    def connect(self, from_node, to_node):
        if from_node not in self.nodes:
            log(f'{from_node} not added to flowgraph {self}', 'error')
            raise(f'{from_node} not added to flowgraph {self}')
        if to_node not in self.nodes:
            log(f'{to_node} not added to flowgraph {self}', 'error')
            raise(f'{to_node} not added to flowgraph {self}')

        from_node.connect(to_node)
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

    def register_signal_handler(self, signals, node):
        if node in self.nodes:
            for signal in signals:
                if signal.name() in self.signals:
                    for s in self.signals[signal.name()]:
                        s.register(signal, node.handle_signal)
        else:
            raise ValueError("Node is not in flowgraph")

    def __str__(self):
        return self.name