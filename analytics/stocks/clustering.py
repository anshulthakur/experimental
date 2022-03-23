
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


import django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'rest.settings')
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from stocks.models import Listing, Stock

#Get all the stocks listing
#Create a composite dataframe of closing prices for all securities
#and compute correlation across all of them (and cluster them together)

'''
def get_stock_listing(stock):
    print(stock.sid)
    listing = Listing.objects.filter(stock=stock)
    df = read_frame(listing, fieldnames=['closing', 'date'], index_col='date')
    for column in df.columns:
        if column != 'stock':
           df[column] = pd.to_numeric(df[column])
    df = df.sort_index()
    df = df.reindex(columns = ['closing'])
    df.rename(columns={"closing":stock.sid.replace(' ', '_')}, inplace=True)
    #Optionally, filter out by date range
    start_date = '2020-01-01'
    end_date = '2020-12-31'
    df = df.loc[start_date:end_date]
    return df
'''

def get_stock_listing(stock):
    import datetime
    print(stock.sid)
    first_date = datetime.date(2020, 1, 1)
    last_date = datetime.date(2020, 12, 31)
    listing = Listing.objects.filter(stock=stock, date__range=(first_date, last_date))
    df = read_frame(listing, fieldnames=['closing', 'date'], index_col='date')
    for column in df.columns:
        if column != 'stock':
           df[column] = pd.to_numeric(df[column])
    df = df.sort_index()
    df = df.reindex(columns = ['closing'])
    df.rename(columns={"closing":stock.sid.replace(' ', '_')}, inplace=True)
    #Optionally, filter out by date range
    #start_date = '2020-01-01'
    #end_date = '2021-12-31'
    #df = df.loc[start_date:end_date]
    return df



stocks = Stock.objects.all()

read_list = []
with open('read_stocks.txt', 'r') as fd:
    for line in fd:
        read_list.append(line.strip())

count = 0
#store = pd.HDFStore('store.h5')

path = 'store.h5'
df = pd.DataFrame()
if len(read_list) < len(stocks):
    chunk = (len(read_list)//100)+1
for stock in stocks:
    if stock.sid.strip() in read_list:
        print('Skip')
        continue
    if count == 100:
        fd = open('read_stocks.txt', 'w')
        for sid in read_list:
            fd.write('{}\n'.format(sid))
        fd.close()
        with pd.HDFStore(path) as store:
            print('Save chunk_{}'.format(chunk))
            store['chunk_{}'.format(chunk)] = df  # save it
        chunk += 1
        df = pd.DataFrame()
        count = 0

    if stock.name != 'NIFTY' and stock.name != 'BANKNIFTY':
        sdf = get_stock_listing(stock)
        if len(sdf>0):
            #df = df.append(sdf)
            df = pd.concat([df, sdf])
            count += 1
            read_list.append(stock.sid.strip())
fd = open('read_stocks.txt', 'w')
for sid in read_list:
    fd.write('{}\n'.format(sid))
fd.close()
with pd.HDFStore(path) as store:
    print('Save chunk_{}'.format(chunk))
    store['chunk_{}'.format(chunk)] = df  # save it

#for col in df.columns:
#    print(col)


#store['df']  # load it
print('Done collecting. Condensing')

print('Chunks={}'.format(chunk))
with pd.HDFStore(path) as store:
    df = store['chunk_1']
    for chunk in range(2, chunk+1):
        df = pd.concat[df, store['chunk_{}'.format(chunk)]]

    store['df'] = df
