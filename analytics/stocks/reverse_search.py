'''
Created on 01-Sep-2022

@author: Anshul
'''

import os
import sys
import csv

from lib.tradingview import TvDatafeed, Interval
import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
 
nse_list = 'NSE_list.csv'
cache_dir = './images/cache/'
try:
    os.mkdir(cache_dir)
except FileExistsError:
    pass
except:
    print('Error creating folder')

indices = []

with open(nse_list, 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        indices.append(row['SYMBOL'].strip())

tvfeed_instance = None

def get_tvfeed_instance(username, password):
    global tvfeed_instance
    if tvfeed_instance is None:
        tvfeed_instance = TvDatafeed(username, password)
    return tvfeed_instance


def cached(name, df=None):
    import json
    cache_file = '.cache.json'
    overwrite = False
    try:
        with open(cache_dir+cache_file, 'r') as fd:
            progress = json.load(fd)
            try:
                date = datetime.datetime.strptime(progress['date'], '%d-%m-%Y')
                if date.day == datetime.datetime.today().day and \
                    date.month == datetime.datetime.today().month and \
                    date.year == datetime.datetime.today().year:
                    pass #Cache hit
                else:
                    if df is None:#Cache is outdated. Clear it first
                        for f in os.listdir(cache_dir):
                            if f != cache_dir+cache_file:
                                os.remove(os.path.join(cache_dir, f))
                    overwrite = True
            except:
                #Doesn't look like a proper date time
                pass
    except:
        overwrite=True
    
    if overwrite:
        with open(cache_dir+cache_file, 'w') as fd:
            fd.write(json.dumps({'date':datetime.datetime.today().strftime('%d-%m-%Y')}))
    
    f = cache_dir+name+'.csv'
    if df is None:
        if os.path.isfile(f):
            #Get from cache if it exists
            df = pd.read_csv(f)
            df['datetime'] = pd.to_datetime(df['datetime'], format='%Y-%m-%d %H:%M:%S')
            return df
        return None
    else:
        #Cache the results
        df.to_csv(f)
        return None

username = 'AnshulBot'
password = '@nshulthakur123'
tv = get_tvfeed_instance(username, password)

cutoff_date = datetime.datetime.strptime('01-Sep-2013', "%d-%b-%Y")
d = relativedelta(datetime.datetime.today(), cutoff_date)
n_bars = (d.years*12) + d.months
for stock in indices:
    print(stock)
    symbol = stock.strip().replace('&', '_')
    symbol = symbol.replace('-', '_')
    nse_map = {'UNITDSPR': 'MCDOWELL_N',
               'MOTHERSUMI': 'MSUMI'}
    if symbol in nse_map:
        symbol = nse_map[symbol]
    
    s_df = cached(symbol)
    if s_df is not None:
        pass
    else:
        s_df = tv.get_hist(
                    symbol,
                    'NSE',
                    interval=Interval.in_monthly,
                    n_bars=n_bars,
                    extended_session=False,
                )
        if s_df is not None:
            cached(symbol, s_df)