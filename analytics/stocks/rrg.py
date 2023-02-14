'''
Created on 02-May-2022

@author: anshul
'''

import os
import sys
import settings
import matplotlib
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

from django_pandas.io import read_frame
from mplfinance.original_flavor import candlestick_ohlc
import matplotlib.dates as mpl_dates

from matplotlib.dates import date2num

# imports
import pandas_datareader.data as pdr
import datetime
import talib
from talib.abstract import *
from talib import MA_Type

from lib.tradingview import TvDatafeed, Interval, convert_timeframe_to_quant
from download_index_reports import download_historical_data

#Prepare to load stock data as pandas dataframe from source. In this case, prepare django
import django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'rest.settings')
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from stocks.models import Listing, Stock, Market

#Libraries for the Plotting
from bokeh.plotting import figure
from bokeh.io import show, save, output_file
from bokeh.models import ColumnDataSource
from bokeh.models.widgets import DataTable, TableColumn
from pandas.tseries.frequencies import to_offset

import holoviews as hv
from holoviews import opts, dim

import panel as pn
import csv


INDICES = ["Nifty_50",
           "Nifty_Auto",
           "Nifty_Bank",
           "Nifty_Energy",
           "Nifty_Financial_Services",
           "Nifty_FMCG",
           "Nifty_IT",
           "Nifty_Media",
           "Nifty_Metal",
           "Nifty_MNC",
           "Nifty_Pharma",
           "Nifty_PSU_Bank",
           "Nifty_Realty",
           "Nifty_India_Consumption",
           "Nifty_Commodities",
           "Nifty_Infrastructure",
           "Nifty_PSE",
           "Nifty_Services_Sector",
           "Nifty_Growth_Sectors_15",
           "NIFTY_SME_EMERGE",
           "Nifty_Oil_&_Gas",
           "Nifty_Healthcare_Index",
           "Nifty_Total_Market",
           "Nifty_India_Digital",
           "Nifty_Mobility",
           "Nifty_India_Defence",
           "Nifty_Financial_Services_Ex_Bank",
           "Nifty_Housing",
           "Nifty_Transportation_&_Logistics",
           "Nifty_MidSmall_Financial_Services",
           "Nifty_MidSmall_Healthcare",
           "Nifty_MidSmall_IT_&_Telecom",
           "Nifty_Consumer_Durables",
           "Nifty_Non_Cyclical_Consumer",
           "Nifty_India_Manufacturing",
           "Nifty_Next_50",
           "Nifty_100",
           "Nifty_200",
           "Nifty_500",
           "Nifty_Midcap_50",
           "NIFTY_Midcap_100",
           "NIFTY_Smallcap_100",
           #"Nifty_Dividend_Opportunities_50",
           #"Nifty_Low_Volatility_50",
           #"Nifty_Alpha_50",
           #"Nifty_High_Beta_50",
           "Nifty100_Equal_Weight",
           "Nifty100_Liquid_15",
           "Nifty_CPSE",
           "Nifty50_Value_20",
           "Nifty_Midcap_Liquid_15",
           "NIFTY100_Quality_30",
           "Nifty_Private_Bank",
           "Nifty_Smallcap_250",
           "Nifty_Smallcap_50",
           "Nifty_MidSmallcap_400",
           "Nifty_Midcap_150",
           "Nifty_Midcap_Select",
           "NIFTY_LargeMidcap_250",
           "Nifty_Financial_Services_25_50",
           "Nifty500_Multicap_50_25_25",
           "Nifty_Microcap_250",
           "Nifty200_Momentum_30",
           "NIFTY100_Alpha_30",
           "NIFTY500_Value_50",
           "Nifty100_Low_Volatility_30",
           "NIFTY_Alpha_Low_Volatility_30",
           "NIFTY_Quality_Low_Volatility_30",
           "NIFTY_Alpha_Quality_Low_Volatility_30",
           "NIFTY_Alpha_Quality_Value_Low_Volatility_30",
           "NIFTY200_Quality_30",
           "NIFTY_Midcap150_Quality_50",
           "Nifty200_Alpha_30",
           "Nifty_Midcap150_Momentum_50",
           "NIFTY50_Equal_Weight",
           ]

index_data_dir = './reports/'
member_dir = './reports/members/'
plotpath = index_data_dir+'plots/'
cache_dir = index_data_dir+'cache/'

tvfeed_instance = None

def load_progress():
    import json
    progress_file = '.progress.json'
    
    try:
        with open(progress_file, 'r') as fd:
            progress = json.load(fd)
            try:
                date = datetime.datetime.strptime(progress['date'], '%d-%m-%Y')
                if date.day == datetime.datetime.today().day and \
                    date.month == datetime.datetime.today().month and \
                    date.year == datetime.datetime.today().year:
                    return progress['index']
            except:
                #Doesn't look like a proper date time
                pass
    except:
        pass
    
    return []

def save_progress(index):
    import json
    progress_file = '.progress.json'
    
    processed = load_progress()
    if len(processed) == 0:
        processed = [index]
    else:
        processed.append(index)
    with open(progress_file, 'w') as fd:
            fd.write(json.dumps({'date':datetime.datetime.today().strftime('%d-%m-%Y'),
                                 'index': processed}))
    return

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
def get_tvfeed_instance(username, password):
    global tvfeed_instance
    if tvfeed_instance is None:
        tvfeed_instance = TvDatafeed(username, password)
    return tvfeed_instance
        
MEMBER_MAP = {
    "auto":"ind_niftyautolist.csv",
    "healthcare":"ind_niftyhealthcarelist.csv",
    "metal":"ind_niftymetallist.csv",
    "banknifty":"ind_niftybanklist.csv",
    "housing":"ind_niftyhousing_list.csv",
    "oilgas":"ind_niftyoilgaslist.csv",
    "commodities":"ind_niftycommoditieslist.csv",
    "defense":"ind_niftyindiadefence_list.csv",
    "pharma":"ind_niftypharmalist.csv",
    "consumer":"ind_niftyconsumerdurableslist.csv",
    "digital":"ind_niftyindiadigital_list.csv",
    "pvtbank":"ind_niftyprivatebanklist.csv",
    "consumption":"ind_niftyconsumptionlist.csv",
    "manufacturing":"ind_niftyindiamanufacturing_list.csv",
    "psubank":"ind_niftypsubanklist.csv",
    "energy":"ind_niftyenergylist.csv",
    "infra":"ind_niftyinfralist.csv",
    "realty":"ind_niftyrealtylist.csv",
    "finance":"ind_niftyfinancelist.csv",
    "niftyit":"ind_niftyitlist.csv",
    "services":"ind_niftyservicelist.csv",
    "fmcg":"ind_niftyfmcglist.csv",
    "niftymedia":"ind_niftymedialist.csv",
    "logistics":"ind_niftytransportationandlogistics_list.csv",
    "noncyclical":"ind_niftynoncyclicalconsumer_list.csv",
    "finexbank":"ind_niftyfinancialservicesexbank_list.csv",
}


hv.extension('bokeh')

# format price data
pd.options.display.float_format = '{:0.2f}'.format

import os
import sys
import settings
import csv

from lib.retrieval import get_stock_listing

def load_members(sector, members, date, sampling='w', entries=50, online=True):
    print('========================')
    print(f'Loading for {sector}')
    print('========================')
    
    df = pd.read_csv(f'{index_data_dir}{sector}.csv')
    df.rename(columns={'Index Date': 'date',
                       'Closing Index Value': sector},
               inplace = True)
    df['date'] = pd.to_datetime(df['date'], format='%d-%m-%Y')
    df.set_index('date', inplace = True)
    df = df.sort_index()
    df = df.reindex(columns = [sector])
    df = df[~df.index.duplicated(keep='first')]
    
    if date is not None:
        df = df[:date.strftime('%Y-%m-%d')]
    if sampling=='w':
        #Resample weekly
        logic = {}
        for cols in df.columns:
            if cols != 'date':
                logic[cols] = 'last'
        #Resample on weekly levels
        df = df.resample('W').apply(logic)
        #df = df.resample('W-FRI', closed='left').apply(logic)
        df.index -= to_offset("6D")
    #Truncate to last n days
    df = df.iloc[-entries:]
    #print(df.head(10))
    #print(date)
    start_date = df.index.values[0]
    end_date = df.index.values[-1]
    #print(start_date, type(start_date))

    #print(np.datetime64(date))
    duration = np.datetime64(datetime.datetime.today())-start_date
    if sampling=='w':
        duration = duration.astype('timedelta64[W]')/np.timedelta64(1, 'W')
    else:
        duration = duration.astype('timedelta64[D]')/np.timedelta64(1, 'D')
    
    duration = max(int(duration.astype(int))+1, entries)

    username = 'AnshulBot'
    password = '@nshulthakur123'
    tv = None
    interval = convert_timeframe_to_quant(sampling)
    if online:
        tv = get_tvfeed_instance(username, password)
    #print(duration, type(duration))
    for stock in members:
        try:
            if not online:
                market = Market.objects.get(name='NSE')
                stock_obj = Stock.objects.get(symbol=stock, market=market)
                s_df = get_stock_listing(stock_obj, duration=duration, last_date = date)
                s_df = s_df.drop(columns = ['open', 'high', 'low', 'volume', 'delivery', 'trades'])
                #print(s_df.head())
                if len(s_df)==0:
                    print('Skip {}'.format(stock_obj))
                    continue
                s_df.rename(columns={'close': stock},
                           inplace = True)
                s_df.reset_index(inplace = True)
                s_df['date'] = pd.to_datetime(s_df['date'], format='%d-%m-%Y')
                #s_df.drop_duplicates(inplace = True, subset='date')
                s_df.set_index('date', inplace = True)
                s_df = s_df.sort_index()
                s_df = s_df.reindex(columns = [stock])
                s_df = s_df[~s_df.index.duplicated(keep='first')]
                #print(s_df[s_df.index.duplicated(keep=False)])
                s_df = s_df.loc[pd.to_datetime(start_date).date():pd.to_datetime(end_date).date()]
                df[stock] = s_df[stock]
            else:
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
                                interval=interval,
                                n_bars=duration,
                                extended_session=False,
                            )
                    if s_df is not None:
                        cached(symbol, s_df)
                if s_df is None:
                    print(f'Error fetching information on {symbol}')
                else:
                    s_df = s_df.drop(columns = ['open', 'high', 'low', 'volume'])
                    #print(s_df.head())
                    if len(s_df)==0:
                        print('Skip {}'.format(symbol))
                        continue
                    s_df.reset_index(inplace = True)
                    s_df.rename(columns={'close': stock, 'datetime': 'date'},
                               inplace = True)
                    #print(s_df.columns)
                    #pd.to_datetime(df['DateTime']).dt.date
                    s_df['date'] = pd.to_datetime(s_df['date'], format='%d-%m-%Y').dt.date
                    #s_df.drop_duplicates(inplace = True, subset='date')
                    s_df.set_index('date', inplace = True)
                    s_df = s_df.sort_index()
                    s_df = s_df.reindex(columns = [stock])
                    s_df = s_df[~s_df.index.duplicated(keep='first')]
                    #print(s_df.index.values[0], type(s_df.index.values[0]))
                    #print(pd.to_datetime(start_date).date(), type(pd.to_datetime(start_date).date()))
                    s_df = s_df.loc[pd.to_datetime(start_date).date():pd.to_datetime(end_date).date()]
                    #print(s_df.loc[start_date:end_date])
                    #print(s_df.head(10))
                    #print(s_df[s_df.index.duplicated(keep=False)])
                    df[stock] = s_df[stock]
        except Stock.DoesNotExist:
            print(f'{stock} values do not exist')
    df = df[~df.index.duplicated(keep='first')]
    
    #print(df.head(10))
    return df

def compute_jdk(benchmark = 'Nifty_50', base_df=None):
    #print(base_df.head(10))
    df = base_df.copy(deep=True)
    
    df.sort_values(by='date', inplace=True, ascending=True)
    #Drop all columns which don't have a valid first row
    for cols in df.columns:
        #print(f'{cols}: {df[cols].isnull().sum()}')
        if np.isnan(df[cols].iloc[0]):
            print('Drop {}'.format(cols))
            df = df.drop(columns = cols)
    #Calculate the 1-day Returns for the Indices
    df = df.pct_change(1)
    #print(df.tail())
    #Calculate the Indices' value on and Index-Base (100) considering the calculated returns
    df.iloc[0] = 100
    for ticker in df.columns:
        for i in range(1, len(df[ticker])):
            df[ticker][i] = df[ticker][i-1]*(1+df[ticker][i])
    #Define the Index for comparison (Benchamrk Index): Nifty50
    #print(f'Benchmark: {benchmark}')
    benchmark_values = df[benchmark]
    #print(df.tail())
    df = df.drop(columns = benchmark)
    #print(df.tail())
    #print(len(df))
    #Calculate the relative Performance of the Index in relation to the Benchmark
    for ticker in df.columns:   
        df[ticker] = df[ticker]/benchmark_values - 1
    
    #Normalize the Values considering a 14-days Window (Note: 10 weekdays)
    for ticker in df.columns:
        df[ticker] = 100 + ((df[ticker] - df[ticker].rolling(10).mean())/df[ticker].rolling(10).std() + 1)

    # Rouding and Excluding NA's
    #print(df.head())
    df = df.round(2).dropna()
    
    #print(df.tail())

    #Compute on the last few dates only (last 5 days)
    JDK_RS_ratio = df.iloc[-25:]
    
    #Calculate the Momentum of the RS-ratio
    #JDK_RS_momentum = JDK_RS_ratio.pct_change(10)
    JDK_RS_momentum = JDK_RS_ratio.pct_change(4)
    
    #Normalize the Values considering a 14-days Window (Note: 10 weekdays)
    for ticker in JDK_RS_momentum.columns: 
        JDK_RS_momentum[ticker] = 100 + ((JDK_RS_momentum[ticker] - JDK_RS_momentum[ticker].rolling(10).mean())/JDK_RS_momentum[ticker].rolling(10).std() + 1)
    #print(JDK_RS_momentum.tail())
    # Rounding and Excluding NA's
    
    JDK_RS_momentum = JDK_RS_momentum.round(2).dropna()
    
    #Adjust DataFrames to be shown in Monthly terms
    #JDK_RS_ratio = JDK_RS_ratio.reset_index()
    #JDK_RS_ratio['date'] = pd.to_datetime(JDK_RS_ratio['date'], format='%Y-%m-%d')
    #JDK_RS_ratio = JDK_RS_ratio.set_index('date')
    #JDK_RS_ratio = JDK_RS_ratio.resample('M').ffill()

    #... now for JDK_RS Momentum
    #JDK_RS_momentum = JDK_RS_momentum.reset_index()
    #JDK_RS_momentum['date'] = pd.to_datetime(JDK_RS_momentum['date'], format='%Y-%m-%d')
    #JDK_RS_momentum = JDK_RS_momentum.set_index('date')
    #JDK_RS_momentum = JDK_RS_momentum.resample('M').ffill()
    
    #print('JDK')
    #print(JDK_RS_ratio.head())
    #print('Momentum')
    #print(JDK_RS_momentum.head())
    
    return [JDK_RS_ratio, JDK_RS_momentum]
    

def load_file_list(directory="./indices/"):
    file_list = []
    for filename in os.listdir(directory):
        f = os.path.join(directory, filename)
        # checking if it is a file
        if os.path.isfile(f) and f.endswith('.csv'):
            file_list.append(f)
    return file_list

def load_sectoral_indices(date, sampling, entries=50):
    from pathlib import Path
    df = pd.read_csv(index_data_dir+'Nifty_50.csv')
    df.rename(columns={'Index Date': 'date',
                       'Closing Index Value': 'Nifty_50'},
               inplace = True)
    df['date'] = pd.to_datetime(df['date'], format='%d-%m-%Y')
    df.set_index('date', inplace = True)
    df = df.sort_index()
    df = df.reindex(columns = ['Nifty_50'])
    #filelist = load_file_list()

    #print(df.head())
    for index in INDICES:
        f = '{}{}.csv'.format(index_data_dir, index)
        #print('Reading: {}'.format(f))
        #index = Path(f).stem.strip().lower()
        if index == "Nifty_50":
            continue
        s_df = pd.read_csv(f)
        s_df.rename(columns={'Index Date': 'date',
                             'Closing Index Value': index},
                   inplace = True)
        s_df['date'] = pd.to_datetime(s_df['date'], format='%d-%m-%Y')
        #s_df.drop_duplicates(inplace = True, subset='date')
        s_df.set_index('date', inplace = True)
        s_df = s_df.sort_index()
        s_df = s_df.reindex(columns = [index])
        s_df = s_df[~s_df.index.duplicated(keep='first')]
        #print(s_df[s_df.index.duplicated(keep=False)])
        df[index] = s_df[index]
    df = df[~df.index.duplicated(keep='first')]
    if date is not None:
        df = df[:date.strftime('%Y-%m-%d')]
    if sampling=='w':
        #Resample weekly
        logic = {}
        for cols in df.columns:
            if cols != 'date':
                logic[cols] = 'last'
        #Resample on weekly levels
        df = df.resample('W').apply(logic)
        #df = df.resample('W-FRI', closed='left').apply(logic)
        df.index -= to_offset("6D")
    return df.tail(entries)

def load_index_members(name):
    members = []
    if name not in INDICES:#MEMBER_MAP:
        print(f'{name} not in list')
        return members
    #with open('./indices/members/'+MEMBER_MAP[name], 'r', newline='') as csvfile:
    with open(f'{member_dir}{name}.csv', 'r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            members.append(row['Symbol'].strip())
    return members

def save_scatter_plots(JDK_RS_ratio, JDK_RS_momentum, sector='unnamed'):
    # Create the DataFrames for Creating the ScaterPlots
    #Create a Sub-Header to the DataFrame: 'JDK_RS_ratio' -> As later both RS_ratio and RS_momentum will be joint
    JDK_RS_ratio_subheader = pd.DataFrame(np.zeros((1,JDK_RS_ratio.columns.shape[0])),columns=JDK_RS_ratio.columns)
    JDK_RS_ratio_subheader.iloc[0] = 'JDK_RS_ratio'

    JDK_RS_ratio_total = pd.concat([JDK_RS_ratio_subheader, JDK_RS_ratio], axis=0)

    #... same for JDK_RS Momentum
    JDK_RS_momentum_subheader = pd.DataFrame(np.zeros((1,JDK_RS_momentum.columns.shape[0])),columns=JDK_RS_momentum.columns)
    JDK_RS_momentum_subheader.iloc[0] = 'JDK_RS_momentum'

    JDK_RS_momentum_total = pd.concat([JDK_RS_momentum_subheader, JDK_RS_momentum], axis=0)

    #Join both DataFrames
    RRG_df = pd.concat([JDK_RS_ratio_total, JDK_RS_momentum_total], axis=1, sort=True)
    RRG_df = RRG_df.sort_index(axis=1)
    
    #Create a DataFrame Just with the Last Period Metrics for Plotting the Scatter plot
    ##Reduce JDK_RS_ratio to 1 (Last) Period
    JDK_RS_ratio_1P = pd.DataFrame(JDK_RS_ratio.iloc[-1].transpose())
    JDK_RS_ratio_1P = JDK_RS_ratio_1P.rename(columns= {JDK_RS_ratio_1P.columns[0]: 'JDK_RS_ratio'})
    
    ##Reduce JDK_RS_momentum to 1 (Last) Period
    JDK_RS_momentum_1P = pd.DataFrame(JDK_RS_momentum.iloc[-1].transpose())
    JDK_RS_momentum_1P = JDK_RS_momentum_1P.rename(columns= {JDK_RS_momentum_1P.columns[0]: 'JDK_RS_momentum'})
    
    #Joining the 2 Dataframes
    JDK_RS_1P = pd.concat([JDK_RS_ratio_1P,JDK_RS_momentum_1P], axis=1)
    
    ##Reset the Index so the Index's names are in the Scatter
    JDK_RS_1P = JDK_RS_1P.reset_index() 
    order = [1,2,0] # setting column's order
    JDK_RS_1P = JDK_RS_1P[[JDK_RS_1P.columns[i] for i in order]]
    
    ##Create a New Column with the Quadrants Indication
    JDK_RS_1P['Quadrant'] = JDK_RS_1P['index']
    for row in JDK_RS_1P['Quadrant'].index:
        if JDK_RS_1P['JDK_RS_ratio'][row] > 100 and JDK_RS_1P['JDK_RS_momentum'][row] > 100:
            JDK_RS_1P['Quadrant'][row] = 'Leading'
        elif JDK_RS_1P['JDK_RS_ratio'][row] > 100 and JDK_RS_1P['JDK_RS_momentum'][row] < 100:
            JDK_RS_1P['Quadrant'][row] = 'Lagging'
        elif JDK_RS_1P['JDK_RS_ratio'][row] < 100 and JDK_RS_1P['JDK_RS_momentum'][row] < 100:
            JDK_RS_1P['Quadrant'][row] = 'Weakening'
        elif JDK_RS_1P['JDK_RS_ratio'][row] < 100 and JDK_RS_1P['JDK_RS_momentum'][row] > 100:
            JDK_RS_1P['Quadrant'][row] = 'Improving'
    #Scatter Plot
    #scatter = hv.Scatter(JDK_RS_1P, kdims = ['JDK_RS_ratio', 'JDK_RS_momentum'])
    scatter = hv.Scatter(JDK_RS_1P, kdims = ['JDK_RS_momentum'])
    #scatter = JDK_RS_1P.plot.scatter('JDK_RS_ratio', 'JDK_RS_momentum')
    
    ##Colors
    explicit_mapping = {'Leading': 'green', 'Lagging': 'yellow', 'Weakening': 'red', 'Improving': 'blue'}
    
    ##Defining the Charts's Area
    x_max_distance = max(abs(int(JDK_RS_1P['JDK_RS_ratio'].min())-100), int(JDK_RS_1P['JDK_RS_ratio'].max())-100,
                        abs(int(JDK_RS_1P['JDK_RS_momentum'].min())-100), int(JDK_RS_1P['JDK_RS_momentum'].max())-100)
    x_y_range = (100 - 1 - x_max_distance, 100 + 1 + x_max_distance)
    
    ##Plot Joining all together
    scatter = scatter.opts(opts.Scatter(tools=['hover'], height = 500, width=500, size = 10, xlim = x_y_range, ylim = x_y_range,
                                       color = 'Quadrant', cmap=explicit_mapping, legend_position = 'top'))
    
    ##Vertical and Horizontal Lines
    vline = hv.VLine(100).opts(color = 'black', line_width = 1)
    hline = hv.HLine(100).opts(color = 'black', line_width = 1)
    
    #All Together
    
    full_scatter = scatter * vline * hline
    #Let's use the Panel library to be able to save the Table generated
    p = pn.panel(full_scatter)
    p.save(plotpath+sector+'_ScatterPlot_1Period.html') 
    
    #For multiple period we need to create a DataFrame with 3-dimensions 
    #-> to do this we create a dictionary and include each DataFrame with the assigned dictionary key being the Index
    indices =  RRG_df.columns.unique()

    multi_df = dict()
    for index in indices:
        #For each of the Index will do the following procedure

        chosen_columns = []
        #This loop is to filter each variable's varlue in the big-dataframe and create a create a single Dataframe
        for column in RRG_df[index].columns:
            chosen_columns.append(RRG_df[index][column])
        joint_table = pd.concat(chosen_columns, axis=1)

        #Change the DataFrame's Header
        new_header = joint_table.iloc[0] 
        joint_table = joint_table[1:] 
        joint_table.columns = new_header
        joint_table = joint_table.loc[:,~joint_table.columns.duplicated()]

        #Remove the first 3 entries
        joint_table = joint_table[2:]

        #Create a column for the Index
        joint_table['index'] = index

        ##Reset the Index so the Datess are observable the Scatter
        joint_table = joint_table.reset_index()
        order = [1,2,3,0] # setting column's order
        joint_table = joint_table[[joint_table.columns[i] for i in order]]
        joint_table = joint_table.rename(columns={"level_0": "Date"})
        joint_table['Date'] = joint_table['Date'].apply(lambda x: x.strftime('%Y-%m-%d'))

        ##Create a New Column with the Quadrants Indication
        joint_table['Quadrant'] = joint_table['index']
        for row in joint_table['Quadrant'].index:
            if joint_table['JDK_RS_ratio'][row] >= 100 and joint_table['JDK_RS_momentum'][row] >= 100:
                joint_table['Quadrant'][row] = 'Leading'
            elif joint_table['JDK_RS_ratio'][row] >= 100 and joint_table['JDK_RS_momentum'][row] <= 100:
                joint_table['Quadrant'][row] = 'Lagging'
            elif joint_table['JDK_RS_ratio'][row] <= 100 and joint_table['JDK_RS_momentum'][row] <= 100:
                joint_table['Quadrant'][row] = 'Weakening'
            elif joint_table['JDK_RS_ratio'][row] <= 100 and joint_table['JDK_RS_momentum'][row] >= 100:
                joint_table['Quadrant'][row] = 'Improving'

        #Joining the obtained Single Dataframes into the Dicitonary
        multi_df.update({index: joint_table})  
    #Defining the Charts's Area
    x_y_max = []
    for Index in multi_df.keys():
        x_y_max_ = max(abs(int(multi_df[Index]['JDK_RS_ratio'].min())-100), int(multi_df[Index]['JDK_RS_ratio'].max())-100,
                        abs(int(multi_df[Index]['JDK_RS_momentum'].min())-100), int(multi_df[Index]['JDK_RS_momentum'].max())-100)
        x_y_max.append(x_y_max_)

    x_range = (100 - 1 - max(x_y_max), 100 + 1 + max(x_y_max))
    y_range = (100 - 1 - max(x_y_max), 100 + 1.25 + max(x_y_max))
    #Note: y_range has .25 extra on top because legend stays on top and option "legend_position" doesn't exist for Overlay graphs

    indices_name = RRG_df.columns.drop_duplicates().tolist()

    #Include Dropdown List
    def load_indices(Index): 
        #scatter = hv.Scatter(multi_df[Index], kdims = ['JDK_RS_ratio', 'JDK_RS_momentum'])
        scatter = hv.Scatter(multi_df[Index], kdims = ['JDK_RS_momentum'])
    
        ##Colors
        explicit_mapping = {'Leading': 'green', 'Lagging': 'yellow', 'Weakening': 'red', 'Improving': 'blue'}
        ##Plot Joining all together
        scatter = scatter.opts(opts.Scatter(tools=['hover'], height = 500, width=500, size = 10, xlim = x_range, ylim = y_range,
                                            color = 'Quadrant', cmap=explicit_mapping,
                                           ))
    
        ##Line connecting the dots
        #curve = hv.Curve(multi_df[Index], kdims = ['JDK_RS_ratio', 'JDK_RS_momentum'])
        curve = hv.Curve(multi_df[Index], kdims = [ 'JDK_RS_momentum'])
        curve = curve.opts(opts.Curve(color = 'black', line_width = 1))
    
        ##Vertical and Horizontal Lines
        vline = hv.VLine(100).opts(color = 'black', line_width = 1)
        hline = hv.HLine(100).opts(color = 'black', line_width = 1)    
    
    
        #All Together
    
        full_scatter = scatter * vline * hline * curve
        full_scatter = full_scatter.opts(legend_cols= True)
    
        return full_scatter
    #Instantiation the Dynamic Map object
    dmap = hv.DynamicMap(load_indices, kdims='Index').redim.values(Index=indices_name)
    
    #Let's use the Panel library to be able to save the Table generated
    p = pn.panel(dmap)
    p.save(plotpath+sector+'_ScatterPlot_Multiple_Period.html', embed = True) 
    
    

def main(date=datetime.date.today(), sampling = 'w', online=True):
    try:
        os.mkdir(cache_dir)
    except FileExistsError:
        pass
    except:
        print('Error creating folder')
        
    processed = load_progress()
    #print(processed)
    df = load_sectoral_indices(date, sampling, entries=33)
    benchmark = 'Nifty_50'
    [JDK_RS_ratio, JDK_RS_momentum] = compute_jdk(benchmark=benchmark, base_df = df)
    save_scatter_plots(JDK_RS_ratio, JDK_RS_momentum, benchmark)
    if len(JDK_RS_ratio) >0:
        for col in JDK_RS_ratio:
            if JDK_RS_ratio.iloc[-1][col] > 100 and len(JDK_RS_momentum) >0 and JDK_RS_momentum.iloc[-1][col] > 100:
                print(f'{col} is leading [RS:{JDK_RS_ratio.iloc[-1][col]} MOM:{JDK_RS_momentum.iloc[-1][col]}]')
            elif JDK_RS_ratio.iloc[-1][col] < 100 and len(JDK_RS_momentum) >0 and JDK_RS_momentum.iloc[-1][col] > 100:
                print(f'{col} is improving [RS:{JDK_RS_ratio.iloc[-1][col]} MOM:{JDK_RS_momentum.iloc[-1][col]}]')
            elif JDK_RS_ratio.iloc[-1][col] < 100 and len(JDK_RS_momentum) >0 and JDK_RS_momentum.iloc[-1][col] < 100:
                print(f'{col} is weakening [RS:{JDK_RS_ratio.iloc[-1][col]} MOM:{JDK_RS_momentum.iloc[-1][col]}]')
            elif JDK_RS_ratio.iloc[-1][col] > 100 and len(JDK_RS_momentum) >0 and JDK_RS_momentum.iloc[-1][col] < 100:
                print(f'{col} is lagging [RS:{JDK_RS_ratio.iloc[-1][col]} MOM:{JDK_RS_momentum.iloc[-1][col]}]')
            elif len(JDK_RS_momentum)==0:
                print(f'{benchmark} has NaN values')
            else:
                print(f'{col}')
    else:
        print(f'{benchmark} has NaN values in ratio')
    
    #Whichever sectors are leading, find the strongest stock in those
    for column in JDK_RS_ratio.columns:
        #if JDK_RS_ratio.iloc[-1][column] > 100 and JDK_RS_momentum.iloc[-1][column] > 100:
        if column in processed:
            print(f'Skip {column}. Already processed for the day')
            continue
        members = load_index_members(column)
        if len(members) ==0:
            continue
        df = load_members(sector=column, members=members, date=date, sampling=sampling, entries=33, online=online)
        #print(df.head())
        [ratio, momentum] = compute_jdk(benchmark=column, base_df = df)
        save_scatter_plots(ratio, momentum, column)
        if len(ratio) >0:
            for col in ratio:
                if ratio.iloc[-1][col] > 100 and len(momentum) >0 and momentum.iloc[-1][col] > 100:
                    print(f'{col} is leading [RS:{ratio.iloc[-1][col]} MOM:{momentum.iloc[-1][col]}]')
                elif ratio.iloc[-1][col] < 100 and len(momentum) >0 and momentum.iloc[-1][col] > 100:
                    print(f'{col} is improving [RS:{ratio.iloc[-1][col]} MOM:{momentum.iloc[-1][col]}]')
                elif ratio.iloc[-1][col] < 100 and len(momentum) >0 and momentum.iloc[-1][col] < 100:
                    print(f'{col} is weakening [RS:{ratio.iloc[-1][col]} MOM:{momentum.iloc[-1][col]}]')
                elif ratio.iloc[-1][col] > 100 and len(momentum) >0 and momentum.iloc[-1][col] < 100:
                    print(f'{col} is lagging [RS:{ratio.iloc[-1][col]} MOM:{momentum.iloc[-1][col]}]')
                elif len(momentum)==0:
                    print(f'{column} has NaN values')
                else:
                    print(f'{col}')
        else:
            print(f'{column} has NaN values in ratio')
        save_progress(column)
                
if __name__ == "__main__":
    day = datetime.date.today()
    import argparse
    parser = argparse.ArgumentParser(description='Compute RRG data for indices')
    parser.add_argument('-d', '--daily', action='store_true', default = False, help="Compute RRG on daily TF")
    parser.add_argument('-w', '--weekly', action='store_true', default = True, help="Compute RRG on weekly TF")
    parser.add_argument('-o', '--online', action='store_true', default = False, help="Fetch data from TradingView (Online)")
    parser.add_argument('-f', '--for', dest='date', help="Compute RRG for date")
    #Can add options for weekly sampling and monthly sampling later
    args = parser.parse_args()
    stock_code = None
    sampling = 'w'
    if args.daily:
        sampling='d'
    if args.date is not None and len(args.date)>0:
        print('Get data for date: {}'.format(args.date))
        day = datetime.datetime.strptime(args.date, "%d/%m/%y")
    
    pd.set_option("display.precision", 8)
    download_historical_data(day, silent=True)
    main(date=day, sampling=sampling, online=args.online)
    