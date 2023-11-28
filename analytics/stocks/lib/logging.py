loglevel = 'info'
logger = None
loglevels = ['error', 'warning', 'info', 'debug']
def set_loglevel(level):
    global loglevel
    loglevel = level
    if logger is not None:
        logger.set_loglevel(level)

def log(args, logtype='info'):
    global loglevels
    global loglevel
    if loglevels.index(logtype) <= loglevels.index(loglevel):
        print(args)

class Logger(object):
    def __init__(self, level='info', name=None, **kwargs):
        self.level = level
        self.name = name
        print('Create logger')

    def log(self, *args):
        log(*args, logtype=self.level)
    
    def debug(self, *args):
        log(*args, logtype="debug")
    
    def warning(self, *args):
        log(*args, logtype="warning")

    def error(self, *args):
        log(*args, logtype="error")

    def info(self, *args):
        log(*args, logtype="info")

    def set_loglevel(self, level):
        self.level = level

def getLogger(name=None):
    global logger
    if logger is not None:
        logger = Logger(name)
        logger.info('Created logger')
    return logger