from lib.logging import log
import copy
class FlowGraphNode(object):
    def __init__(self, name=None, connections=[]):
        self.callbacks = []
        self.connections = copy.deepcopy(connections)
        self._flowgraph = None
        self.is_root = True
        self.name = name
        if name is None:
            raise Exception('name must be provided')
        self.mode = None

    @property
    def flowgraph(self):
        return self._flowgraph

    @flowgraph.setter
    def flowgraph(self, flowgraph):
        self._flowgraph = flowgraph

    def connect(self, node):
        log(f'Connect {node} to {self}', 'debug')
        self.connections.append(node)

    async def emit(self, signal_data):
        for callback in self.callbacks:
            callback(signal_data)

    async def register(self, callback):
        self.callbacks.append(callback)

    def display_connections(self):
        for connection in self.connections:
            if self.is_root:
                print(f"\n{self}", end=" ")
            print(f"-> {connection}", end=" ")
            connection.display_connections()

    async def next(self, **kwargs):
        pass

    def __str__(self):
        return self.name


class FlowGraph(object):
    def __init__(self, name=None, frequency=None, mode='buffered'):
        self.name = name
        self._frequency = frequency
        self.nodes = []
        self.roots = []
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
            await node.next(**kwargs)

    def display(self):
        # code for displaying the flowgraph goes here
        for node in self.roots:
            node.display_connections()
        print('\n')

    async def emit(self, signal_data, emitting_node):
        for node in self.nodes:
            for callback in node.callbacks:
                await callback(signal_data, emitting_node)

    async def register(self, callback, registering_node):
        if registering_node in self.nodes:
            registering_node.callbacks.append(callback)
        else:
            raise ValueError("Node is not in flowgraph")

    def __str__(self):
        return self.name