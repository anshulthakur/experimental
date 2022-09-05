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
bse_list = 'BSE_list.csv'
img_dir = './images/'
cache_dir = './images/cache/'
try:
    os.mkdir(cache_dir)
except FileExistsError:
    pass
except:
    print('Error creating folder')

indices = []
b_indices = []
with open(nse_list, 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        indices.append(row['SYMBOL'].strip())

with open(bse_list, 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        if row['Security Id'].strip() not in indices:
            b_indices.append(row['Security Id'].strip())


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

# Load the reference candlestick chart
r_df = pd.read_csv(img_dir+'2.csv')
r_df = r_df.drop(columns = ['Candle Color','Candle Length','open','close'])
#print(s_df.head())
r_df.reset_index(inplace = True)
r_df['date'] = pd.to_datetime(r_df['date'], format='%d/%m/%Y').dt.date
r_df.set_index('date', inplace = True)
r_df = r_df.sort_index()
r_df = r_df.reindex(columns = ['change'])

print(len(r_df))
r_df.drop(r_df.iloc[len(r_df)-1].name, inplace=True) #Last entry is the month which may still be running

start_date = r_df.index.values[0]
end_date = r_df.index.values[-1]

r_df.drop(r_df.iloc[0].name, inplace=True) #First entry is not the change, just the baseline

username = 'AnshulBot'
password = '@nshulthakur123'
tv = get_tvfeed_instance(username, password)

cutoff_date = datetime.datetime.strptime('01-Aug-2013', "%d-%b-%Y")
d = relativedelta(datetime.datetime.today(), cutoff_date)
n_bars = (d.years*12) + d.months

correlations = []
bse_correlations = []
ii=0
max_corr = 0
max_corr_idx = None
for stock in indices:
    print(f'{stock} Max: {max_corr_idx}({max_corr})')
    symbol = stock.strip().replace('&', '_')
    symbol = symbol.replace('-', '_')
    symbol = symbol.replace('*', '')
    nse_map = {'UNITDSPR': 'MCDOWELL_N',
               'MOTHERSUMI': 'MSUMI'}
    if symbol in nse_map:
        symbol = nse_map[symbol]
    
    s_df = cached(symbol)
    if s_df is not None:
        #print('Found in Cache')
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
    if s_df is not None:
        s_df = s_df.drop(columns = ['open', 'high', 'low', 'volume'])
        #print(s_df.head())
        if len(s_df)==0:
            print('Skip {}'.format(symbol))
            continue
        s_df.reset_index(inplace = True)
        s_df.rename(columns={'datetime': 'date', 'close': 'change'},
                   inplace = True)
        s_df['date'] = pd.to_datetime(s_df['date'], format='%d-%m-%Y').dt.date
        s_df.set_index('date', inplace = True)
        s_df = s_df.sort_index()
        s_df = s_df.reindex(columns = ['change'])
        s_df = s_df[~s_df.index.duplicated(keep='first')]
        s_df = s_df.loc[pd.to_datetime(start_date).date():pd.to_datetime(end_date).date()]
        s_df = s_df.pct_change(1)
        
        if len(s_df)<len(r_df)+1:
            #print('Skip')
            correlations.append(0)
        else:
            s_df.drop(s_df.iloc[0].name, inplace=True) #First entry is going to be NaN
            c = r_df.iloc[:,0].corr(s_df.iloc[:,0])
            #print(f'Correlation: {c}')
            correlations.append(c)
            if c>=max_corr:
                max_corr=c
                max_corr_idx = symbol

for stock in b_indices:
    print(f'{stock} Max: {max_corr_idx}({max_corr})')
    symbol = stock.strip().replace('&', '_')
    symbol = symbol.replace('-', '_')
    symbol = symbol.replace('*', '')
    
    s_df = cached(symbol)
    if s_df is not None:
        #print('Found in Cache')
        pass
    else:
        s_df = tv.get_hist(
                    symbol,
                    'BSE',
                    interval=Interval.in_monthly,
                    n_bars=n_bars,
                    extended_session=False,
                )
        if s_df is not None:
            cached(symbol, s_df)
    if s_df is not None:
        s_df = s_df.drop(columns = ['open', 'high', 'low', 'volume'])
        #print(s_df.head())
        if len(s_df)==0:
            print('Skip {}'.format(symbol))
            continue
        s_df.reset_index(inplace = True)
        s_df.rename(columns={'datetime': 'date', 'close': 'change'},
                   inplace = True)
        s_df['date'] = pd.to_datetime(s_df['date'], format='%d-%m-%Y').dt.date
        s_df.set_index('date', inplace = True)
        s_df = s_df.sort_index()
        s_df = s_df.reindex(columns = ['change'])
        s_df = s_df[~s_df.index.duplicated(keep='first')]
        s_df = s_df.loc[pd.to_datetime(start_date).date():pd.to_datetime(end_date).date()]
        s_df = s_df.pct_change(1)
        
        if len(s_df)<len(r_df)+1:
            #print('Skip')
            bse_correlations.append(0)
        else:
            s_df.drop(s_df.iloc[0].name, inplace=True) #First entry is going to be NaN
            c = r_df.iloc[:,0].corr(s_df.iloc[:,0])
            #print(f'Correlation: {c}')
            bse_correlations.append(c)
            if c>=max_corr:
                max_corr=c
                max_corr_idx = symbol
val = max(correlations)
max_idx = [index for index, item in enumerate(correlations) if item == max(correlations)]
names = [indices[idx] for idx in max_idx]
print(f'Maximum correlation (NSE):{val}: {names}')

val = max(bse_correlations)
max_idx = [index for index, item in enumerate(bse_correlations) if item == max(bse_correlations)]
names = [b_indices[idx] for idx in max_idx]
print(f'Maximum correlation (BSE):{val}: {names}')
print(f'NSE: {len(indices)}. BSE: {len(b_indices)}')