import os, sys
import csv
import numpy as np
import pandas as pd
from lib.tradingview import get_tvfeed_instance, Interval, convert_timeframe_to_quant
from lib.retrieval import get_stock_listing
from lib.logging import set_loglevel, log
import datetime
from lib.cache import cached
from stocks.models import Listing, Stock, Market

watchlist_file = './reports/watchlist_performance.csv'

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

def main(file):
    #Read watchlist file and for each entry, fetch current price and update row
    df = pd.read_csv(file, index_col='id', dtype={'initial_price': float,
                                                            'cmp': float,
                                                            'high': float,
                                                            'low': float,
                                                            'returns': float})
    df['date_added'] = pd.to_datetime(df['date_added'], format='%Y-%m-%d')
    for ii in range(0, len(df)):
        log(f"{df.loc[ii ,'market'].strip()}:{df.loc[ii, 'symbol'].strip()}", logtype='info')
        stock = get_dataframe(df.loc[ii, 'symbol'].strip().upper(), 
                              df.loc[ii, 'market'].strip().upper(), 
                              timeframe=Interval.in_daily,
                              offline = False,
                              duration=500)
        stock.reset_index(inplace=True)
        stock.set_index('datetime', inplace=True)
        stock = stock.sort_index()
        if stock is None:
            log(f"Could not fetch data for {df.loc[ii, 'symbol']}", logtype='error')
            continue
        if pd.to_datetime(stock.index[0]) < pd.to_datetime(df.loc[ii, 'date_added']):
            stock = stock[stock.index > df.loc[ii, 'date_added']]
        if(len(stock)>0):
            df.loc[ii, 'cmp'] = stock.iloc[len(stock)-1]['close']
            df.loc[ii, 'high'] = stock['high'].max()
            df.loc[ii, 'low'] = stock['low'].min()
            df.loc[ii, 'returns'] = ((df.loc[ii, 'cmp']-df.loc[ii, 'initial_price'])/df.loc[ii, 'initial_price'])*100
            df.loc[ii, 'daily_change'] = ((stock.iloc[len(stock)-1]['close'] - stock.iloc[len(stock)-2]['close'])/stock.iloc[len(stock)-2]['close'])*100
    df.to_csv(file)

if __name__ == "__main__":
    set_loglevel('info')
    import argparse
    parser = argparse.ArgumentParser(description='Update watchlist file with performance report of stocks')
    parser.add_argument('-f', '--file', help="Watchlist file to track")
    args = parser.parse_args()

    main(file=watchlist_file)