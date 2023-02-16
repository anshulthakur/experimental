'''
Created on 30-Jul-2022

@author: anshul
'''

import socket
import os

REMOTE_SERVER = "one.one.one.one"
def is_connected(hostname):
    try:
        # see if we can resolve the host name -- tells us if there is
        # a DNS listening
        host = socket.gethostbyname(hostname)
        # connect to the host -- tells us if the host is actually reachable
        s = socket.create_connection((host, 80), 2)
        s.close()
        return True
    except Exception:
        pass # we ignore any errors, returning False
    return False

def create_directory(path):
    try:
        os.mkdir(path)
    except FileExistsError:
        pass

def get_filelist(folder):
    files = os.listdir(folder)
    files = [f for f in files if os.path.isfile(folder+'/'+f) and f[-3:].strip().lower()=='csv'] #Filtering only the files.
    return files