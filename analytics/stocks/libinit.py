initialized = False

def is_initialized():
    global initialized
    return initialized

def initialize():
    global initialized
    initialized = True