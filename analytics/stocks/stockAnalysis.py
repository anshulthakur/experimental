import os
import sys
import settings
from datetime import datetime

from stocks.models import Listing, Industry, Stock

import pandas as pd
from matplotlib import pyplot

#import sqlite3
#conn = sqlite3.connect("/home/craft/workarea/bsedata.db")

#Method 1

#cur = conn.cursor()

#cur.execute('select * from stocks_stock;')
#results = cur.fetchall()
#print(results)
#cur.close()
#conn.close()


#Method 2
#Somehow, can't use a.group in a single query. Some problem occurs
#stocks = pd.read_sql_query("SELECT a.symbol, a.sid, a.name, a.face_value, a.isin, b.name FROM stocks_stock AS a INNER JOIN stocks_industry AS b ON a.industry_id=b.id;", conn)
#print(stocks)

#listings = pd.read_sql_query("SELECT a.date, a.open, a.high, a.low, a.close, a.wap, a.traded, a.trades, a.turnover, a.deliverable, a.ratio, a.spread_high_low, a.spread_close_open, b.name FROM stocks_listing AS a INNER JOIN stocks_stock AS b ON a.stock_id=b.id;", conn)
#print(listings)

#Method 3
from django_pandas.io import read_frame
stock = Stock.objects.get(sid='ASIANPAINT')
listings = Listing.objects.filter(stock=stock).order_by('date')

#df = read_frame(listings) #cf https://stackoverflow.com/questions/22898824/filtering-pandas-dataframes-on-dates
df = read_frame(listings, index_col='date', coerce_float=True, fieldnames=['date', 'open', 'close', 'high', 'low', 'wap', 'traded', 'trades', 'turnover', 'deliverable', 'ratio', 'spread_high_low', 'spread_close_open'])
#print(df)


def plot_prices(df, from_date=None, to_date=None):
  if from_date==None:
    from_date = df.index[0]
  if to_date==None:
    to_date = df.index[-1]

  filtered_vals = df.loc[from_date:to_date] #With date as index, we access it with .loc attribute
  filtered_vals.close.plot()
  filtered_vals.open.plot()

pyplot.figure()
#plot_prices(df, from_date='2018-12-01', to_date='2018-12-31')
plot_prices(df)
pyplot.legend()
pyplot.show()


#Covariance over a period:
#Take two stocks and find the covariance of the two over a period
stock = Stock.objects.get(sid='ASIANPAINT')
listings = Listing.objects.filter(stock=stock).order_by('date')
stock_1 = read_frame(listings, index_col='date', coerce_float=True, fieldnames=['date', 'open', 'close', 'high', 'low', 'wap', 'traded', 'trades', 'turnover', 'deliverable', 'ratio', 'spread_high_low', 'spread_close_open'])

stock = Stock.objects.get(sid='BAJAJELEC')
listings = Listing.objects.filter(stock=stock).order_by('date')
stock_2 = read_frame(listings, index_col='date', coerce_float=True, fieldnames=['date', 'open', 'close', 'high', 'low', 'wap', 'traded', 'trades', 'turnover', 'deliverable', 'ratio', 'spread_high_low', 'spread_close_open'])

def fixup_series(stock_1, stock_2):
  """
     There could be days when one stock traded but the other didn't. 
     In such cases, insert 0 entries in the one where entry doesn't exist.
  """
  indices_1 = set(stock_1.index.tolist())
  indices_2 = set(stock_2.index.tolist())

  dummy_entry = { 'date': None,
                  'open': 0,
                  'close' : 0,
                  'high':0,
                  'low':0,
                  'wap':0,
                  'traded':0,
                  'trades':0,
                  'turnover':0,
                  'deliverable':0,
                  'ratio':0,
                  'spread_close_open':0,
                  'spread_high_low':0,
                }
  diff = indices_1.difference(indices_2)
  for index in diff:
    #dummy_entry['date'] = stock_1.index[stock_1.index.get_loc(index)-1]
    dummy_entry['open'] = stock_2.loc[stock_1.index[stock_1.index.get_loc(index)-1]]['close']
    dummy_entry['close'] = stock_2.loc[stock_1.index[stock_1.index.get_loc(index)-1]]['close']
    dummy_entry['high'] = stock_2.loc[stock_1.index[stock_1.index.get_loc(index)-1]]['close']
    dummy_entry['low'] = stock_2.loc[stock_1.index[stock_1.index.get_loc(index)-1]]['close']
    #stock_2.loc[index] = dummy_entry
    for key in dummy_entry:
      stock_2.loc[index, key] = dummy_entry[key]
    #stock_2 = stock_2.append(dummy_entry)

  diff = indices_2.difference(indices_1)
  for index in diff:
    #dummy_entry['date'] = stock_2.index[stock_2.index.get_loc(index)-1]
    dummy_entry['open'] = stock_1.loc[stock_2.index[stock_2.index.get_loc(index)-1]]['close']
    dummy_entry['close'] = stock_1.loc[stock_2.index[stock_2.index.get_loc(index)-1]]['close']
    dummy_entry['high'] = stock_1.loc[stock_2.index[stock_2.index.get_loc(index)-1]]['close']
    dummy_entry['low'] = stock_1.loc[stock_2.index[stock_2.index.get_loc(index)-1]]['close']
    #stock_1.loc[index] = dummy_entry
    for key in dummy_entry:
      stock_1.loc[index, key] = dummy_entry[key]
    #stock_1 = stock_1.append(dummy_entry)

  return (stock_1, stock_2)


def compute_covariance(stock_1, stock_2, from_date=None, to_date=None):
  s1_mean = stock_1['roi'].mean()
  s2_mean = stock_2['roi'].mean()

  df = pd.DataFrame()
  df['s1'] = stock_1['roi']
  df['s2'] = stock_2['roi']
  cov = df.cov()
  print(cov)

(stock_1, stock_2) = fixup_series(stock_1, stock_2)

#Compute ROI for each day
stock_1['roi'] = stock_1['close'] - stock_1['close'].shift(1)
stock_1.loc[stock_1.index[0]]['roi'] = stock_1.loc[stock_1.index[0]]['close'] - stock_1.loc[stock_1.index[0]]['open']
stock_2['roi'] = stock_2['close'] - stock_2['close'].shift(1)
stock_2.loc[stock_2.index[0]]['roi'] = stock_2.loc[stock_2.index[0]]['close'] - stock_2.loc[stock_2.index[0]]['open']
#print(stock_1.loc[stock_1.index[0]])
#print(stock_1.loc[stock_1.index[1]])

compute_covariance(stock_1, stock_2)

#OK. Now do it for all the stocks
def fixup_series_all(series):
  """
     There could be days when one stock traded but the other didn't. 
     In such cases, insert 0 entries in the one where entry doesn't exist.
  """
  for ii in range(0, len(names)):
    stock_1 = series[ii]
    for jj in range(0, len(names)):
      stock_2 = series[jj]
      if ii == jj:
        continue
      indices_1 = set(stock_1.index.tolist())
      indices_2 = set(stock_2.index.tolist())

      dummy_entry = { #'date': None,
                      'open': 0,
                      'close' : 0,
                      'high':0,
                      'low':0,
                      'wap':0,
                      'traded':0,
                      'trades':0,
                      'turnover':0,
                      'deliverable':0,
                      'ratio':0,
                      'spread_close_open':0,
                      'spread_high_low':0,
                    }
      diff = sorted(indices_1.difference(indices_2)) #Set doesn't sort and we'd like sorted lists
      #print(names[ii], names[jj])
      #print(diff)
      for index in diff:
        if stock_1.index.get_loc(index)==0:
          #First date does not exist, use all zeros (not listed/not traded on day 1 or required interval)
          dummy_entry = { #'date': None,
                      'open': 0,
                      'close' : 0,
                      'high':0,
                      'low':0,
                      'wap':0,
                      'traded':0,
                      'trades':0,
                      'turnover':0,
                      'deliverable':0,
                      'ratio':0,
                      'spread_close_open':0,
                      'spread_high_low':0,
                    }

        else:
          #print(index)
          #print(stock_1.index[stock_1.index.get_loc(index)])
          #print(stock_1.index[stock_1.index.get_loc(index)-1])
          #dummy_entry['date'] = stock_1.index[stock_1.index.get_loc(index)-1]
          dummy_entry['open'] = stock_2.loc[stock_1.index[stock_1.index.get_loc(index)-1]]['close']
          dummy_entry['close'] = stock_2.loc[stock_1.index[stock_1.index.get_loc(index)-1]]['close']
          dummy_entry['high'] = stock_2.loc[stock_1.index[stock_1.index.get_loc(index)-1]]['close']
          dummy_entry['low'] = stock_2.loc[stock_1.index[stock_1.index.get_loc(index)-1]]['close']
        #stock_2.loc[index] = dummy_entry
        for key in dummy_entry:
          stock_2.loc[index, key] = dummy_entry[key]
        #stock_2 = stock_2.append(dummy_entry)

      diff = sorted(indices_2.difference(indices_1))
      for index in diff:
        if stock_2.index.get_loc(index)==0:
          dummy_entry = { #'date': None,
                      'open': 0,
                      'close' : 0,
                      'high':0,
                      'low':0,
                      'wap':0,
                      'traded':0,
                      'trades':0,
                      'turnover':0,
                      'deliverable':0,
                      'ratio':0,
                      'spread_close_open':0,
                      'spread_high_low':0,
                    }
        else:
          #dummy_entry['date'] = stock_2.index[stock_2.index.get_loc(index)-1]
          dummy_entry['open'] = stock_1.loc[stock_2.index[stock_2.index.get_loc(index)-1]]['close']
          dummy_entry['close'] = stock_1.loc[stock_2.index[stock_2.index.get_loc(index)-1]]['close']
          dummy_entry['high'] = stock_1.loc[stock_2.index[stock_2.index.get_loc(index)-1]]['close']
          dummy_entry['low'] = stock_1.loc[stock_2.index[stock_2.index.get_loc(index)-1]]['close']
          #stock_1.loc[index] = dummy_entry
        for key in dummy_entry:
          stock_1.loc[index, key] = dummy_entry[key]
        #stock_1 = stock_1.append(dummy_entry)
  return (series)

def covariance(names, series, from_date=None, to_date=None):
  df = pd.DataFrame()
  for name in names:
    df['name'] = series[names.index(name)]['roi']
  cov = df.cov()
  return cov

stock_qs = Stock.objects.all()
names=[]
series=[]
start_date='2018-01-01'
end_date = '2018-11-30'
for stock in stock_qs:
  names.append(stock.sid)
  listings = Listing.objects.filter(stock=stock).order_by('date')
  df = read_frame(listings, index_col='date', coerce_float=True, fieldnames=['date', 'open', 'close', 'high', 'low', 'wap', 'traded', 'trades', 'turnover', 'deliverable', 'ratio', 'spread_high_low', 'spread_close_open'])[start_date:end_date]
  series.append(df.sort_index())

series = fixup_series_all(series)
for index in range(0, len(names)):
  try:
    stock = series[index]
    stock['roi'] = stock['close'] - stock['close'].shift(1)
    stock.loc[stock.index[0]]['roi'] = stock.loc[stock.index[0]]['close'] - stock.loc[stock.index[0]]['open']
  except:
    print('Error in {} Index {}'.format(names[index], index))

covariance_matrix = covariance(names, series)
print(covariance_matrix[0][:])
