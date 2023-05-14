'''
Created on 01-Sep-2022

@brief Read in all the chart patterns from the reference folder and find new stocks matching them

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
import matplotlib.pyplot as plt
from sklearn.metrics import mean_squared_error
from lib.misc import create_directory, get_filelist
from lib.retrieval import get_stock_listing
from lib.logging import set_loglevel, log
from lib.cache import cached
from stocks.models import Listing, Stock, Market

nse_list = 'NSE_list.csv'
bse_list = 'BSE_list.csv'
img_dir = './images/'
cache_dir = './images/cache/'

tvfeed_instance = None

max_depth = 5

def get_tvfeed_instance(username, password):
    global tvfeed_instance
    if tvfeed_instance is None:
        tvfeed_instance = TvDatafeed(username, password)
    return tvfeed_instance

import numpy as np

def calc_correlation(actual, predic):
    a_diff = actual - np.mean(actual)
    p_diff = predic - np.mean(predic)
    numerator = np.sum(a_diff * p_diff)
    denominator = np.sqrt(np.sum(a_diff ** 2)) * np.sqrt(np.sum(p_diff ** 2))
    return numerator / denominator

def calc_mape(actual, predic):
    return np.mean(np.abs((actual - predic) / actual))

def calc_rmse(actual, predic):
    #print(actual)
    #print(predic)
    #print(actual.mean())
    #print(predic.mean())
    return mean_squared_error(actual - actual.mean(), predic - predic.mean())

def cached_old(name, df=None, timeframe=Interval.in_daily):
    import json
    cache_file = '.cache.json'
    overwrite = False
    try:
        with open(cache_dir+'/'+str(timeframe.value)+'/'+cache_file, 'r') as fd:
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
        with open(cache_dir+'/'+str(timeframe.value)+'/'+cache_file, 'w') as fd:
            fd.write(json.dumps({'date':datetime.datetime.today().strftime('%d-%m-%Y')}))
    
    f = cache_dir+'/'+str(timeframe.value)+'/'+name+'.csv'
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

def emit_plot(a,b):
    plt.figure(figsize=(16, 8), dpi=150)
    #print(len(a[0]), len(b[0]))
    #a[0].reset_index()
    ax = (a[0]).plot(y=a[1], label='a', color='orange')
    (b[0]).plot(ax=ax, y=b[1], label='b', color='blue')
    plt.savefig('./images/compare_lines.png')

def compare_stock_info(r_df, s_df, delta, emit=False, logscale=False, match='close'):
    ref_columns = ['open', 'high', 'low', 'close', 'volume'] 
    ref_columns.remove(match)
    s_df = s_df.drop(columns = ref_columns)
    s_df = s_df.sort_index()
       
    s_df.reset_index(inplace = True)

    #print(s_df.head(10))
    if 'datetime' in s_df.columns:
        s_df = s_df.drop(columns='datetime')
    else:
        s_df = s_df.drop(columns='date')
    #s_df.rename(columns={'close': 'change', 'datetime':'date'},
    s_df.rename(columns={match: 'change'},
                inplace = True)
    #s_df['date'] = pd.to_datetime(s_df['date'], format='%d-%m-%Y').dt.date
    #s_df.set_index('date', inplace = True)
    #s_df = s_df.sort_index()
    s_df = s_df.reindex(columns = ['change'])
    s_df = s_df[~s_df.index.duplicated(keep='first')]
    #s_df = s_df.loc[pd.to_datetime(start_date).date():pd.to_datetime(end_date).date()]
    #s_df = s_df.iloc[-len(r_df)-2:-1]
    
    r_df = r_df - r_df.mean()
    #r_df = r_df.reset_index()
    if len(s_df)<len(r_df)+1:
        #print(f'{len(s_df)},{len(r_df)}Skip')
        #correlations.append(0)
        c = -1
        pass
    else:
        #s_df.drop(s_df.iloc[0].name, inplace=True) #First entry is going to be NaN
        c = 0
        #print('Window slide length {}'.format(len(s_df) - len(r_df)))
        for ii in range(0, max(len(s_df) - len(r_df), max_depth)):
            #print(-(len(r_df)-ii)-1, -ii)
            if ii==0:
                temp_df = s_df.iloc[-(len(r_df)+ii):].copy(deep=True).reset_index().drop(columns = 'index')
                if logscale:
                    temp_df = np.log10(temp_df)
                temp_df = temp_df - temp_df.mean()
            else:
                temp_df = s_df.iloc[-(len(r_df)+ii):-ii].copy(deep=True).reset_index().drop(columns = 'index')
                if logscale:
                    temp_df = np.log10(temp_df)
                temp_df = temp_df - temp_df.mean()
            
            #print(temp_df.tail(10))
            #print(max(temp_df['change']))
            #print(min(temp_df['change']))
            
            temp_df['change'] = temp_df['change']/(max(temp_df['change'] - min(temp_df['change'])))
            
            #print(temp_df.tail(10))
            if delta:
                temp_df = temp_df.pct_change(1)
                temp_df.drop(temp_df.iloc[0].name, inplace=True) #First entry is going to be NaN
            
            if ii==0:
                #print(temp_df.head(10))
                #print(r_df.head(10))
                #print(r_df.iloc[:,0])
                #print(temp_df.iloc[:,0])
                
                if emit:
                    emit_plot([r_df, 'change'], [temp_df, 'change'])
                #plt.figure(figsize=(16, 8), dpi=150)
                #plt.plot(list(range(0, len(r_df.iloc[:,0]))), r_df.iloc[:,0] - np.mean(r_df.iloc[:,0]), label='a', color='orange')
                #plt.plot(list(range(0, len(temp_df.iloc[:,0]))), temp_df.iloc[:,0] -  np.mean(temp_df.iloc[:,0]), label='b', color='green')
                #plt.savefig('./images/compare_lines_1.png')
                pass
            #print(len(r_df), len(temp_df))
            cval = r_df.iloc[:,0].corr(temp_df.iloc[:,0])
            
            #cval = calc_correlation(r_df.iloc[:,0], temp_df.iloc[:,0])
            #mcval = calc_mape(r_df.iloc[:,0], temp_df.iloc[:,0])
            #rmse = calc_rmse(r_df.iloc[:,0], temp_df.iloc[:,0])
            c = max(cval, c)
            
            #print(f'{ii} Correlation: {cval}, {mcval}, {rmse}')
    return c

def get_dataframe(stock, market, timeframe, duration, date=datetime.datetime.now(), offline=False):
    if timeframe not in [Interval.in_3_months,
                         Interval.in_monthly, 
                         Interval.in_weekly, 
                         Interval.in_daily]:
        offline = False


    if not offline:
        username = 'AnshulBot'
        password = '@nshulthakur123'

        tv = get_tvfeed_instance(username, password)
        symbol = stock.strip().replace('&', '_')
        symbol = symbol.replace('-', '_')
        symbol = symbol.replace('*', '')
        nse_map = {'UNITDSPR': 'MCDOWELL_N',
                   'MOTHERSUMI': 'MSUMI'}
        if symbol in nse_map:
            symbol = nse_map[symbol]
        
        s_df = cached(name=symbol, timeframe=timeframe)
        if s_df is not None:
            #print('Found in Cache')
            pass
        else:
            try:
                s_df = tv.get_hist(
                            symbol,
                            market,
                            interval=timeframe,
                            n_bars=duration,
                            extended_session=False,
                        )
                if s_df is not None:
                    cached(name=symbol, df=s_df, timeframe=timeframe)
            except:
                s_df = None
    else:
        try:
            market_obj = Market.objects.get(name=market)
        except Market.DoesNotExist:
            log(f"No object exists for {market}", logtype='error')
            return None
        try:
            stock_obj = Stock.objects.get(symbol=stock, market=market_obj)
        except Stock.DoesNotExist:
            log(f"Stock with symbol {stock} not found in {market}", logtype='error')
            return
        s_df = get_stock_listing(stock_obj, duration=duration, last_date = date, 
                                 resample=True if timeframe in [Interval.in_monthly, 
                                                                Interval.in_weekly] else False, 
                                 monthly=True if timeframe in [Interval.in_monthly] else False)
        s_df = s_df.drop(columns = ['delivery', 'trades'])
    return s_df

def load_references(folder):
    files = get_filelist(folder=folder)
    df_arr = []
    for file in files:
        if file[-3:] == 'csv':
            #print(f'Read {file}')
            r_df = pd.read_csv(os.path.join(folder, file))
            if 'date' in r_df.columns:
                r_df = r_df.drop(columns = ['Candle Color','Candle Length','open','change', 'date'])
            else:
                r_df = r_df.drop(columns = ['Candle Color','Candle Length','open','change'])

            r_df.set_index('index', inplace = True)
            r_df = r_df.sort_index()

            r_df = r_df.reindex(columns = ['close'])
            r_df.rename(columns={'close': 'change'},
                                inplace = True)
            #r_df.drop(r_df.iloc[len(r_df)-1].name, inplace=True) #Last entry is the month which may still be running
            
            #start_date = r_df.index.values[0]
            #end_date = r_df.index.values[-1]
            
            r_df.drop(r_df.iloc[0].name, inplace=True) #First entry is not the change, just the baseline
            r_df.reset_index(inplace = True)
            r_df = r_df.drop(columns=['index'])
            #print(r_df.tail(10))
            #print(len(r_df))
            df_arr.append(r_df)
    print('Loaded references')
    return df_arr

def main(reference, timeframe, logscale=False, match = 'close', offline=False):
    cutoff_date = datetime.datetime.strptime('01-Aug-2018', "%d-%b-%Y")
    delta = False
    try:
        create_directory(cache_dir)
        create_directory(cache_dir+'/'+str(timeframe.value)+'/')
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
    
    # Load the reference candlestick charts from the folder
    df_arr = load_references(reference)
    #r_df = pd.read_csv(reference)
    correlations = []
    bse_correlations = []

    shortlist = {}
    c_thresh = 0.97
    ignore_list = []

    #cutoff_date = r_df.index.values[0]
    for r_df in df_arr:
        d = relativedelta(datetime.datetime.today(), cutoff_date)
        if timeframe == Interval.in_monthly:
            #print('Monthly')
            n_bars = max((d.years*12) + d.months+1, len(r_df))+10
        elif timeframe == Interval.in_3_months:
            #print('3 Monthly')
            n_bars = max((d.years*4) + d.months+1, len(r_df))+10
        elif timeframe == Interval.in_weekly:
            #print('Weekly')
            n_bars = max((d.years*52) + (d.months*5) + d.weeks+1, len(r_df))+10
        elif timeframe == Interval.in_4_hour:
            #print('4 Hourly')
            n_bars = max(500, len(r_df))+10
        elif timeframe == Interval.in_2_hour:
            #print('2 Hourly')
            n_bars = max(500, len(r_df))+10
        elif timeframe == Interval.in_1_hour:
            #print('Hourly')
            n_bars = max(500, len(r_df))+10
        else:
            #print('Daily')
            n_bars = max(500, len(r_df))+10
        
        #print(f'Get {n_bars} candles')
        
        max_corr = 0
        max_corr_idx = None
        
        for stock in indices:
            print('.', end='', flush=True)
            if stock in ignore_list:
                continue
            s_df = get_dataframe(stock=stock, 
                                market='NSE', 
                                timeframe=timeframe, 
                                duration=n_bars, 
                                offline=offline)
            if s_df is not None and len(s_df)>0:
                c = compare_stock_info(r_df, s_df, delta, logscale=logscale, match=match)
                if c >= c_thresh:
                    if stock in shortlist:
                        shortlist[stock] += 1
                    else:
                        shortlist[stock] = 1
                    print(f'\nShortlist: {json.dumps(shortlist, indent=2)}\n')
                if c>max_corr:
                    max_corr=c
                    max_corr_idx = [stock]
                elif c>0 and c==max_corr:
                    max_corr_idx.append(stock)
                #print(f'{stock}[{c}] Max: {max_corr_idx}({max_corr})')
            else:
                ignore_list.append(stock)
        for stock in b_indices:
            print('.', end='', flush=True)
            if stock not in ignore_list:
                continue
            s_df = get_dataframe(stock=stock, 
                                market='BSE', 
                                timeframe=timeframe, 
                                duration=n_bars, 
                                offline=offline)
            if s_df is not None and len(s_df)>0:
                c = compare_stock_info(r_df, s_df, delta, logscale=logscale, match=match)
                if c >= c_thresh:
                    if stock in shortlist:
                        shortlist[stock] += 1
                    else:
                        shortlist[stock] = 1
                    print(f'\nShortlist: {json.dumps(shortlist, indent=2)}\n')
                if c>max_corr:
                    max_corr=c
                    max_corr_idx = [stock]
                elif c>0 and c==max_corr:
                    max_corr_idx.append(stock)
                #print(f'{stock}[{c}] Max: {max_corr_idx}({max_corr})')
            else:
                ignore_list.append(stock)

    #print(f'Maximum correlation:{max_corr}: {max_corr_idx}')
    #print(f'NSE: {len(indices)}. BSE: {len(b_indices)}')

    print(f'\nShortlist: {json.dumps(shortlist, indent=2)}\n')
        
if __name__ == "__main__":
    day = datetime.date.today()
    import argparse
    parser = argparse.ArgumentParser(description='Perform reverse search for indices')
    parser.add_argument('-t', '--timeframe', help="Timeframe")
    parser.add_argument('-f', '--folder', help="Folder containing CSV reference files of the candlesticks patterns to search for")
    parser.add_argument('-l', '--log', action="store_true", default=False, help="Use log scaling for price values ")
    parser.add_argument('-o', '--offline', help="Run the analysis using offline data", action = "store_true", default=False)
    timeframe = '1M'
    reference = './images/references/'
    #Can add options for weekly sampling and monthly sampling later
    args = parser.parse_args()
    if args.folder is not None and len(args.folder)>0:
        print('Reference folder: {}'.format(args.folder))
        reference = args.folder
    if args.timeframe is not None and len(args.timeframe)>0:
        timeframe=args.timeframe

    main(reference, 
         timeframe=convert_timeframe_to_quant(timeframe),
         logscale=args.log, 
         match = 'close',
         offline = args.offline)
