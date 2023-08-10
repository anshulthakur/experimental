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
import matplotlib.pyplot as plt
from sklearn.metrics import mean_squared_error
from lib.misc import create_directory
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
        print(f'{len(s_df)},{len(r_df)}Skip')
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


def main(reference, timeframe, delta, stock=None, logscale=False, 
         cutoff_date = datetime.datetime.strptime('01-Aug-2018', "%d-%b-%Y"), match = 'close', offline=False):
    if delta:
        print('Use delta')
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
    
    # Load the reference candlestick chart
    r_df = pd.read_csv(reference)
    if delta:
        r_df = r_df.drop(columns = ['Candle Color','Candle Length','open','close', 'date'])
    else:
        r_df = r_df.drop(columns = ['Candle Color','Candle Length','open','change', 'date'])
    #print(r_df.head())
    #r_df.reset_index(inplace = True)
    #r_df['date'] = pd.to_datetime(r_df['date'], format='%d/%m/%Y').dt.date
    r_df.set_index('index', inplace = True)
    r_df = r_df.sort_index()
    if delta:
        r_df = r_df.reindex(columns = ['change'])
    else:
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
    
    #cutoff_date = r_df.index.values[0]
    d = relativedelta(datetime.datetime.today(), cutoff_date)
    if timeframe == Interval.in_monthly:
        print('Monthly')
        n_bars = max((d.years*12) + d.months+1, len(r_df))+10
    elif timeframe == Interval.in_3_months:
        print('3 Monthly')
        n_bars = max((d.years*4) + d.months+1, len(r_df))+10
    elif timeframe == Interval.in_weekly:
        print('Weekly')
        n_bars = max((d.years*52) + (d.months*5) + d.weeks+1, len(r_df))+10
    elif timeframe == Interval.in_4_hour:
        print('4 Hourly')
        n_bars = max(500, len(r_df))+10
    elif timeframe == Interval.in_2_hour:
        print('2 Hourly')
        n_bars = max(500, len(r_df))+10
    elif timeframe == Interval.in_1_hour:
        print('Hourly')
        n_bars = max(500, len(r_df))+10
    else:
        print('Daily')
        n_bars = max(500, len(r_df))+10
    
    print(f'Get {n_bars} candles')
    correlations = []
    bse_correlations = []

    shortlist = {}
    c_thresh = 0.95
    max_corr = 0
    max_corr_idx = None
    if stock is not None:
        s_df = get_dataframe(stock=stock, 
                            market='NSE', 
                            timeframe=timeframe, 
                            duration=n_bars, 
                            offline=offline)
        if s_df is not None and len(s_df)>0:
            c = compare_stock_info(r_df, s_df, delta, emit=True, logscale=logscale, match=match)
            print(f'{stock} Max: {max_corr_idx}({max_corr})')
            print(f'Correlation: {c}')
    else:
        for stock in indices:
            s_df = get_dataframe(stock=stock, 
                                 market='NSE', 
                                 timeframe=timeframe, 
                                 duration=n_bars, 
                                 offline=offline)
            if s_df is not None and len(s_df)>0:
                c = compare_stock_info(r_df, s_df, delta, logscale=logscale, match=match)
                if c >= c_thresh:
                    shortlist[stock] = c
                if c>max_corr:
                    max_corr=c
                    max_corr_idx = [stock]
                elif c>0 and c==max_corr:
                    max_corr_idx.append(stock)
                print(f'{stock}[{c}] Max: {max_corr_idx}({max_corr})')
        
        for stock in b_indices:
            s_df = get_dataframe(stock=stock, 
                                 market='BSE', 
                                 timeframe=timeframe, 
                                 duration=n_bars, 
                                 offline=offline)
            if s_df is not None and len(s_df)>0:
                c = compare_stock_info(r_df, s_df, delta, logscale=logscale, match=match)
                if c >= c_thresh:
                    shortlist[stock] = c
                if c>max_corr:
                    max_corr=c
                    max_corr_idx = [stock]
                elif c>0 and c==max_corr:
                    max_corr_idx.append(stock)
                print(f'{stock}[{c}] Max: {max_corr_idx}({max_corr})')
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

        #print(f'Shortlist: {json.dumps(shortlist, indent=2)}')
        print(f'\nShortlist: {sorted( ((v,k) for k,v in shortlist.items()), reverse=True)}')
if __name__ == "__main__":
    day = datetime.date.today()
    import argparse
    parser = argparse.ArgumentParser(description='Perform reverse search for indices')
    parser.add_argument('-t', '--timeframe', help="Timeframe")
    parser.add_argument('-f', '--file', help="CSV file of the candlesticks to search for")
    parser.add_argument('-d', '--delta', action="store_true", default=False, help="Use delta between points to calculate similarity")
    parser.add_argument('-s', '--stock', help="Specify stock to compare with")
    parser.add_argument('-l', '--log', action="store_true", default=False, help="Use log scaling for price values ")
    parser.add_argument('-c', '--cutoff', help="Cutoff date")
    parser.add_argument('-m', '--match', help="Match OHLC (open/high/low/close)", default='close')
    parser.add_argument('-o', '--offline', help="Run the analysis using offline data", action = "store_true", default=False)
    timeframe = '1M'
    reference = None
    stock = None
    #Can add options for weekly sampling and monthly sampling later
    args = parser.parse_args()
    if args.file is not None and len(args.file)>0:
        print('Search stock for file: {}'.format(args.file))
        reference = args.file
    if args.timeframe is not None and len(args.timeframe)>0:
        timeframe=args.timeframe
    if args.stock is not None and len(args.stock)>0:
        stock = args.stock
    if args.cutoff is not None and len(args.cutoff)>0:
        cutoff_date = datetime.datetime.strptime(args.cutoff, "%d-%b-%Y")
    else:
        cutoff_date = datetime.datetime.strptime('01-Aug-2018', "%d-%b-%Y")
    main(reference, 
         timeframe=convert_timeframe_to_quant(timeframe), 
         delta=args.delta, stock=stock, 
         logscale=args.log, 
         cutoff_date=cutoff_date,
         match = args.match,
         offline = args.offline)
