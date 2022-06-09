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

plotpath = './indices/'
#Prepare to load stock data as pandas dataframe from source. In this case, prepare django
import django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'rest.settings')
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from stocks.models import Listing, Stock

#Libraries for the Plotting
from bokeh.plotting import figure
from bokeh.io import show, save, output_file
from bokeh.models import ColumnDataSource
from bokeh.models.widgets import DataTable, TableColumn

import holoviews as hv
from holoviews import opts, dim

import panel as pn

import csv
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

def load_members(sector, members, date):
    print('========================')
    print(f'Loading for {sector}')
    print('========================')
    df = pd.read_csv(f'./indices/{sector}.csv')
    df.rename(columns={'Date': 'date',
                       'Close': sector},
               inplace = True)
    df['date'] = pd.to_datetime(df['date'], format='%d-%b-%Y', infer_datetime_format=True)
    df.set_index('date', inplace = True)
    df = df.sort_index()
    df = df.reindex(columns = [sector])
    #Truncate to last 30 days
    df = df.iloc[-30:]
    #print(date)
    start_date = df.index.values[0]
    #print(start_date, type(start_date))
    #print(np.datetime64(date))
    duration = np.datetime64(date)-start_date
    duration = duration.astype('timedelta64[D]')/np.timedelta64(1, 'D')
    #print(duration, type(duration))
    for stock in members:
        try:
            stock_obj = Stock.objects.get(sid=stock)
            s_df = get_stock_listing(stock_obj, duration=duration, last_date = date)
            s_df = s_df.drop(columns = ['open', 'high', 'low', 'volume', 'delivery', 'trades'])
            #print(s_df.head())
            if len(s_df)==0:
                print('Skip {}'.format(stock_obj))
                continue
            s_df.rename(columns={'close': stock},
                       inplace = True)
            s_df.reset_index(inplace = True)
            s_df['date'] = pd.to_datetime(s_df['date'], format='%d-%b-%Y', infer_datetime_format=True)
            #s_df.drop_duplicates(inplace = True, subset='date')
            s_df.set_index('date', inplace = True)
            s_df = s_df.sort_index()
            s_df = s_df.reindex(columns = [stock])
            s_df = s_df[~s_df.index.duplicated(keep='first')]
            #print(s_df[s_df.index.duplicated(keep=False)])
            df[stock] = s_df[stock]
        except Stock.DoesNotExist:
            print(f'{stock} values do not exist')
    df = df[~df.index.duplicated(keep='first')]
    
    return df

def compute_jdk(benchmark = 'nifty50', base_df=None):
    #print(base_df.tail())
    df = base_df.copy(deep=True)
    
    df.sort_values(by='date', inplace=True, ascending=True)
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
    JDK_RS_momentum = JDK_RS_ratio.pct_change(1)
    #Normalize the Values considering a 14-days Window (Note: 10 weekdays)
    for ticker in JDK_RS_momentum.columns: 
        JDK_RS_momentum[ticker] = 100 + ((JDK_RS_momentum[ticker] - JDK_RS_momentum[ticker].rolling(10).mean())/JDK_RS_momentum[ticker].rolling(10).std() + 1)
    #print(JDK_RS_momentum.tail())
    # Rounding and Excluding NA's
    
    JDK_RS_momentum = JDK_RS_momentum.round(2).dropna()
    
    #Adjust DataFrames to be shown in Monthly terms
    JDK_RS_ratio = JDK_RS_ratio.reset_index()
    JDK_RS_ratio['date'] = pd.to_datetime(JDK_RS_ratio['date'], format='%Y-%m-%d')
    JDK_RS_ratio = JDK_RS_ratio.set_index('date')
    #JDK_RS_ratio = JDK_RS_ratio.resample('M').ffill()

    #... now for JDK_RS Momentum
    JDK_RS_momentum = JDK_RS_momentum.reset_index()
    JDK_RS_momentum['date'] = pd.to_datetime(JDK_RS_momentum['date'], format='%Y-%m-%d')
    JDK_RS_momentum = JDK_RS_momentum.set_index('date')
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

def load_sectoral_indices(date):
    from pathlib import Path
    df = pd.read_csv('./indices/nifty50.csv')
    df.rename(columns={'Date': 'date',
                       'Close': 'nifty50'},
               inplace = True)
    df['date'] = pd.to_datetime(df['date'], format='%d-%b-%Y', infer_datetime_format=True)
    df.set_index('date', inplace = True)
    df = df.sort_index()
    df = df.reindex(columns = ['nifty50'])
    filelist = load_file_list()

    #print(df.head())
    for f in filelist:
        print('Reading: {}'.format(f))
        index = Path(f).stem.strip().lower()
        if index == "nifty50":
            continue
        s_df = pd.read_csv(f)
        s_df.rename(columns={'Date': 'date',
                             'Close': index},
                   inplace = True)
        s_df['date'] = pd.to_datetime(s_df['date'], format='%d-%b-%Y', infer_datetime_format=True)
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
    return df

def load_index_members(name):
    members = []
    if name not in MEMBER_MAP:
        print(f'{name} not in list')
        return members
    with open('./indices/members/'+MEMBER_MAP[name], 'r', newline='') as csvfile:
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
    
    

def main(date=datetime.date.today()):
    df = load_sectoral_indices(date)
    df.sort_values(by='date', inplace=True, ascending=True)
    #Calculate the 1-day Returns for the Indices
    df = df.pct_change(1)
    
    #Calculate the Indices' value on and Index-Base (100) considering the calculated returns
    df.iloc[0] = 100
    for ticker in df.columns:
        for i in range(1, len(df[ticker])):
            df[ticker][i] = df[ticker][i-1]*(1+df[ticker][i])
            
    #Define the Index for comparison (Benchamrk Index): Nifty50
    benchmark = 'nifty50'
    benchmark_values = df[benchmark]
    
    df = df.drop(columns = benchmark)
    
    #Calculate the relative Performance of the Index in relation to the Benchmark
    for ticker in df.columns:   
        df[ticker] = df[ticker]/benchmark_values - 1
    
    #Normalize the Values considering a 14-days Window (Note: 10 weekdays)
    for ticker in df.columns: 
        df[ticker] = 100 + ((df[ticker] - df[ticker].rolling(10).mean())/df[ticker].rolling(10).std() + 1)
        
    # Rouding and Exclusing NA's
    df = df.round(2).dropna()
    
    #Compute on the last few dates only (last 5 days)
    JDK_RS_ratio = df.iloc[-25:]
    
    #Calculate the Momentum of the RS-ratio
    JDK_RS_momentum = JDK_RS_ratio.pct_change(10)
    
    #Normalize the Values considering a 14-days Window (Note: 10 weekdays)
    for ticker in JDK_RS_momentum.columns: 
        JDK_RS_momentum[ticker] = 100 + ((JDK_RS_momentum[ticker] - JDK_RS_momentum[ticker].rolling(10).mean())/JDK_RS_momentum[ticker].rolling(10).std() + 1)
    
    # Rounding and Excluding NA's
    JDK_RS_momentum = JDK_RS_momentum.round(2).dropna()
    
    #Adjust DataFrames to be shown in Monthly terms
    JDK_RS_ratio = JDK_RS_ratio.reset_index()
    JDK_RS_ratio['date'] = pd.to_datetime(JDK_RS_ratio['date'], format='%Y-%m-%d')
    JDK_RS_ratio = JDK_RS_ratio.set_index('date')
    #JDK_RS_ratio = JDK_RS_ratio.resample('M').ffill()
    
    #... now for JDK_RS Momentum
    JDK_RS_momentum = JDK_RS_momentum.reset_index()
    JDK_RS_momentum['date'] = pd.to_datetime(JDK_RS_momentum['date'], format='%Y-%m-%d')
    JDK_RS_momentum = JDK_RS_momentum.set_index('date')
    #JDK_RS_momentum = JDK_RS_momentum.resample('M').ffill()
    
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
    p.save(plotpath+'ScatterPlot_1Period.html') 
    
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
            
    indices_name = RRG_df.columns.drop_duplicates().tolist()
    
    #Instantiation the Dynamic Map object
    dmap = hv.DynamicMap(load_indices, kdims='Index').redim.values(Index=indices_name)
    #Let's use the Panel library to be able to save the Table generated
    p = pn.panel(dmap)
    p.save(plotpath+'ScatterPlot_Multiple_Period.html', embed = True) 
    
    #Sector level RRGs
    
    
    #Whichever sectors are leading, find the strongest stock in those
    for column in JDK_RS_ratio.columns:
        #if JDK_RS_ratio.iloc[-1][column] > 100 and JDK_RS_momentum.iloc[-1][column] > 100:
        members = load_index_members(column)
        if len(members) ==0:
            continue
        df = load_members(column, members, date)
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
                
if __name__ == "__main__":
    day = datetime.date.today()
    import argparse
    parser = argparse.ArgumentParser(description='Compute RRG data for indices')
    parser.add_argument('-d', '--date', help="Date")
    #Can add options for weekly sampling and monthly sampling later
    args = parser.parse_args()
    stock_code = None
    
    if args.date is not None and len(args.date)>0:
        print('Get data for date: {}'.format(args.date))
        day = datetime.datetime.strptime(args.date, "%d/%m/%y")
    pd.set_option("display.precision", 8)
    main(date=day)
    