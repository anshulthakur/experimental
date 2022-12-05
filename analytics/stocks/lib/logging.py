loglevel = 'info'
loglevels = ['error', 'warning', 'info', 'debug']
def set_loglevel(level):
    global loglevel
    loglevel = level

def log(args, logtype='info'):
    global loglevels
    global loglevel
    if loglevels.index(logtype) <= loglevels.index(loglevel):
        print(args)