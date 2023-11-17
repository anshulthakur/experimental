from lib.tradingview import Interval
from lib.misc import create_directory
from lib.logging import log
import datetime
import os
from settings import project_dirs
import pandas as pd

def cached(name, df=None, timeframe=Interval.in_daily):
    import json
    cache_file = '.cache.json'
    overwrite = False
    cache_dir = project_dirs['cache']
    create_directory(cache_dir)
    create_directory(os.path.join(cache_dir,str(timeframe.value)))
    try:
        with open(os.path.join(cache_dir,str(timeframe.value),cache_file), 'r') as fd:
            progress = json.load(fd)
            try:
                date = datetime.datetime.strptime(progress['date'], '%d-%m-%Y')
                if date.day == datetime.datetime.today().day and \
                    date.month == datetime.datetime.today().month and \
                    date.year == datetime.datetime.today().year:
                    log(f'Found {name} in cache', logtype='debug')
                    pass #Cache hit
                else:
                    if df is None:#Cache is outdated. Clear it first
                        for f in os.listdir(os.path.join(cache_dir,str(timeframe.value))):
                            if f != os.path.join(cache_dir,str(timeframe.value),cache_file):
                                #Remove all files except the cache json meta file
                                os.remove(os.path.join(cache_dir,str(timeframe.value), f))
                    overwrite = True
            except:
                #Doesn't look like a proper date time
                pass
    except:
        overwrite=True
    
    if overwrite:
        with open(os.path.join(cache_dir, str(timeframe.value), cache_file), 'w') as fd:
            fd.write(json.dumps({'date':datetime.datetime.today().strftime('%d-%m-%Y')}))
    
    f = os.path.join(cache_dir, str(timeframe.value), name+'.csv')
    if df is None:
        if os.path.isfile(f):
            #Get from cache if it exists
            df = pd.read_csv(f)
            df['datetime'] = pd.to_datetime(df['datetime'], format='%Y-%m-%d %H:%M:%S')
            return df
        return None
    else:
        #Cache the results
        log(f'Add {name} to cache', logtype='debug')
        df.to_csv(f)
        return None

