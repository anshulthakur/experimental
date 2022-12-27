
class AsyncScheduler:
    def __init__(self, interval, simulation=False):
        self.simulation = simulation
        self.interval = interval
        self.flowgraphs = []
    
    def register(self, flowgraph):
        self.flowgraphs.append(flowgraph)
    
    async def run(self):
        for flowgraph in self.flowgraphs:
            await flowgraph.run()
