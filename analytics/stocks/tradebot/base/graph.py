from lib.logging import log

class FlowGraphNode:
    def __init__(self):
        self.callbacks = []
        self._flowgraph = None
    
    @property
    def flowgraph(self):
        return self._flowgraph
    
    @flowgraph.setter
    def set_flowgraph(self, flowgraph):
        self._flowgraph = flowgraph

    async def emit(self, signal_data):
        for callback in self.callbacks:
            callback(signal_data)
    
    async def register(self, callback):
        self.callbacks.append(callback)


class FlowGraph:
    def __init__(self, frequency=None):
        self._frequency = frequency
        self.nodes = []
        self.connections = []
    
    @property
    def frequency(self):
        return self._frequency
    
    @frequency.setter
    def set_frequency(self, frequency):
        if frequency is not None:
            try:
                self._frequency = float(frequency)
            except:
                log(f"Error setting frequency to {frequency}. Ensure that floats are passed", "error")
                pass

    def add_node(self, node):
        if isinstance(node, FlowGraphNode):
            node.flowgraph = self
        else:
            log("Node doesn't have flowgraph attribute", 'error')
            raise Exception('Class of node must be FlowGraphNode')
        self.nodes.append(node)
    
    def connect(self, node1, node2):
        self.connections.append((node1, node2))
    
    def run(self):
        # code for running the flowgraph goes here
        # Start from the first node in the flowgraph and run to completion
        pass
    
    def display(self):
        # code for displaying the flowgraph goes here
        pass

    async def emit(self, signal_data, emitting_node):
        for node in self.nodes:
            for callback in node.callbacks:
                await callback(signal_data, emitting_node)
    
    async def register(self, callback, registering_node):
        if registering_node in self.nodes:
            registering_node.callbacks.append(callback)
        else:
            raise ValueError("Node is not in flowgraph")
