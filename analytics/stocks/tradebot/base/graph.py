from lib.logging import log
import copy
class FlowGraphNode(object):
    def __init__(self, name=None, connections=[]):
        self.callbacks = []
        self.connections = copy.deepcopy(connections)
        self._flowgraph = None
        self.is_root = True
        self.name = name
        self.inputs = {}
        if name is None:
            raise Exception('name must be provided')
        self.mode = None

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
        self.inputs[name] = None

    async def emit(self, signal_data):
        for callback in self.callbacks:
            callback(signal_data)

    async def register(self, callback):
        self.callbacks.append(callback)

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

    def ready(self, connection, **kwargs):
        if self.is_root:
            return True
        if connection is not None:
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
            await node.next(connection=None, **kwargs)

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