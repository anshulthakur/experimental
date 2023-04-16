'''
Created on 30-Jul-2022

@author: anshul
'''

import socket
import os
import datetime
from dateutil.relativedelta import relativedelta
from lib.logging import log 

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
        os.makedirs(path, exist_ok=True)
    except FileExistsError:
        pass

def get_filelist(folder, recursive=False):
    if not recursive:
        files = os.listdir(folder)
        files = [f for f in files if os.path.isfile(folder+'/'+f) and f[-3:].strip().lower()=='csv'] #Filtering only the files.
        return files
    else:
        from os.path import join, getsize
        file_list = []
        for root, dirs, files in os.walk(folder):
            for name in files:
                file_list.append(join(root, name)) 
        return file_list


def create_intraday_folders(base_folder='./'):
    '''
    Need a good way of sorting out intraday data. So, proposed folder and file heirarchy is:

    intraday
    |_ <year>
        |_ <month>
            |_ <day>
                |_ <scrip.csv>
    '''
    date = datetime.datetime.strptime('2012/01', "%Y/%m")
    today = datetime.datetime.today()
    while date.year < today.year or date.month <= today.month:
        create_directory(path = os.path.join(os.path.abspath(base_folder), f'{date.year}/{date.month:02d}'))
        date += relativedelta(months=+1)


