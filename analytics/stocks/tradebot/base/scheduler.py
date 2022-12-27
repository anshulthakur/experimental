
class AsyncScheduler:
    def __init__(self, interval):
        self.interval = interval
        self.flowgraphs = []
    
    async def register(self, flowgraph):
        self.flowgraphs.append(flowgraph)
    
    async def run(self):
        for flowgraph in self.flowgraphs:
            await flowgraph.run()
