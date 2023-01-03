from lib.logging import log
import datetime 
class AsyncScheduler(object):
    def __init__(self, interval, mode='stream'):
        self.mode = mode
        if self.mode not in ['stream', 'buffered', 'backtest']:
            log(f'Unrecognized mode "{self.mode}".', 'error')
            raise Exception(f'Unrecognized mode "{self.mode}".')
        self.interval = interval
        self.flowgraphs = []
        self.running = False
        self.last_runtime = None
    
    def register(self, flowgraph):
        self.flowgraphs.append(flowgraph)

    def change_mode(self, mode):
        if self.mode == 'backtest' or mode=='backtest':
            raise Exception('Cannot change mode from or to Backtest')
        elif self.mode == 'stream' and mode == 'buffered':
            raise Exception('Cannot move into buffered mode from stream mode')
        elif self.mode == 'buffered' and mode == 'stream':
            self.mode = mode
        
    async def stop(self):
        self.running = False
    
    async def run(self):
        self.running = True
        while self.running:
            if self.mode in ['buffered', 'backtest']:
                for flowgraph in self.flowgraphs:
                    await flowgraph.run(tick=datetime.datetime.now())
            else:
                pass
