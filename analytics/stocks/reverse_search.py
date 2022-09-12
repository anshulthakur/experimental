'''
Created on 01-Sep-2022

@author: Anshul
'''

import os
import sys
import csv
import json

from lib.tradingview import TvDatafeed, Interval, convert_timeframe_to_quant
import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
 
nse_list = 'NSE_list.csv'
bse_list = 'BSE_list.csv'
img_dir = './images/'
cache_dir = './images/cache/'

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

def main(reference, timeframe):
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
    
    # Load the reference candlestick chart
    r_df = pd.read_csv(reference)
    r_df = r_df.drop(columns = ['Candle Color','Candle Length','open','close'])
    #print(s_df.head())
    r_df.reset_index(inplace = True)
    #r_df['date'] = pd.to_datetime(r_df['date'], format='%d/%m/%Y').dt.date
    r_df.set_index('index', inplace = True)
    r_df = r_df.sort_index()
    r_df = r_df.reindex(columns = ['change'])
    
    print(len(r_df))
    #r_df.drop(r_df.iloc[len(r_df)-1].name, inplace=True) #Last entry is the month which may still be running
    
    #start_date = r_df.index.values[0]
    #end_date = r_df.index.values[-1]
    
    r_df.drop(r_df.iloc[0].name, inplace=True) #First entry is not the change, just the baseline
    
    username = 'AnshulBot'
    password = '@nshulthakur123'
    tv = get_tvfeed_instance(username, password)
    
    cutoff_date = datetime.datetime.strptime('01-Aug-2018', "%d-%b-%Y")
    #cutoff_date = r_df.index.values[0]
    d = relativedelta(datetime.datetime.today(), cutoff_date)
    if timeframe == Interval.in_monthly:
        print('Monthly')
        n_bars = max((d.years*12) + d.months+1, len(r_df))+10
    elif timeframe == Interval.in_weekly:
        print('Weekly')
        n_bars = max((d.years*52) + (d.months*5) + d.weeks+1, len(r_df))+10
    else:
        print('Daily')
        n_bars = max(500, len(r_df))+10
    
    print(f'Get {n_bars} candles')
    correlations = []
    bse_correlations = []

    shortlist = {}
    c_thresh = 0.85
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
                        interval=timeframe,
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
            s_df = s_df.drop(columns='datetime')
            #s_df.rename(columns={'close': 'change', 'datetime':'date'},
            s_df.rename(columns={'close': 'change'},
                       inplace = True)
            #s_df['date'] = pd.to_datetime(s_df['date'], format='%d-%m-%Y').dt.date
            #s_df.set_index('date', inplace = True)
            s_df = s_df.sort_index()
            s_df = s_df.reindex(columns = ['change'])
            s_df = s_df[~s_df.index.duplicated(keep='first')]
            #s_df = s_df.loc[pd.to_datetime(start_date).date():pd.to_datetime(end_date).date()]
            #s_df = s_df.iloc[-len(r_df)-2:-1]

            #print(s_df.tail(10))
            s_df = s_df.pct_change(1)
            
            if len(s_df)<len(r_df)+1:
                print(f'{len(s_df)},{len(r_df)}Skip')
                #correlations.append(0)
                pass
            else:
                s_df.drop(s_df.iloc[0].name, inplace=True) #First entry is going to be NaN
                c = 0
                for ii in range(0, len(s_df) - len(r_df)):
                    c = max(r_df.iloc[:,0].corr(s_df.iloc[-(len(r_df)-ii):-1,0]), c)
                    #print(f'Correlation: {c}')
                    #correlations.append(c)
                if c >= c_thresh:
                    shortlist[symbol] = c
                if c>max_corr:
                    max_corr=c
                    max_corr_idx = [symbol]
                elif c==max_corr:
                    max_corr_idx.append(symbol)
    
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
                        interval=timeframe,
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
            s_df = s_df.drop(columns='datetime')
            #s_df.rename(columns={'close': 'change', 'datetime':'date'},
            s_df.rename(columns={'close': 'change'},
                       inplace = True)
            #s_df['date'] = pd.to_datetime(s_df['date'], format='%d-%m-%Y').dt.date
            #s_df.set_index('date', inplace = True)
            s_df = s_df.sort_index()
            s_df = s_df.reindex(columns = ['change'])
            s_df = s_df[~s_df.index.duplicated(keep='first')]
            #s_df = s_df.loc[pd.to_datetime(start_date).date():pd.to_datetime(end_date).date()]
            #s_df = s_df.iloc[-len(r_df)-2:-1]
            s_df = s_df.pct_change(1)
            
            if len(s_df)<len(r_df)+1:
                #print('Skip')
                #bse_correlations.append(0)
                pass
            else:
                s_df.drop(s_df.iloc[0].name, inplace=True) #First entry is going to be NaN
                c = 0
                for ii in range(0, len(s_df) - len(r_df)):
                    c = max(r_df.iloc[:,0].corr(s_df.iloc[-(len(r_df)-ii):-1,0]), c)
                    #print(f'Correlation: {c}')
                    #correlations.append(c)
                if c > c_thresh:
                    shortlist[symbol] = c
                if c>max_corr:
                    max_corr=c
                    max_corr_idx = [symbol]
                elif c==max_corr:
                    max_corr_idx.append(symbol)
    #val = max(correlations)
    #max_idx = [index for index, item in enumerate(correlations) if item == max(correlations)]
    #names = [indices[idx] for idx in max_idx]
    #print(f'Maximum correlation (NSE):{val}: {names}')
    
    #val = max(bse_correlations)
    #max_idx = [index for index, item in enumerate(bse_correlations) if item == max(bse_correlations)]
    #names = [b_indices[idx] for idx in max_idx]
    #print(f'Maximum correlation (BSE):{val}: {names}')
    print(f'Maximum correlation:{max_corr}: {max_corr_idx}')
    print(f'NSE: {len(indices)}. BSE: {len(b_indices)}')

    print(f'Shortlist: {json.dumps(shortlist, indent=2)}')
if __name__ == "__main__":
    day = datetime.date.today()
    import argparse
    parser = argparse.ArgumentParser(description='Perform reverse search for indices')
    parser.add_argument('-t', '--timeframe', help="Timeframe")
    parser.add_argument('-f', '--file', help="CSV file of the candlesticks to search for")
    
    timeframe = '1M'
    reference = None
    #Can add options for weekly sampling and monthly sampling later
    args = parser.parse_args()
    if args.file is not None and len(args.file)>0:
        print('Search stock for file: {}'.format(args.file))
        reference = args.file
    if args.timeframe is not None and len(args.timeframe)>0:
        timeframe=args.timeframe

    main(reference, timeframe=convert_timeframe_to_quant(timeframe))
