'''
On a given time scale, determine whether the security is in uptrend, or downtrend, or no-trend
'''
import os
import sys
import settings
import numpy as np
import pandas as pd

import datetime
from stocks.models import Listing, Stock, Market

from lib.retrieval import get_stock_listing
from lib.tradingview import convert_timeframe_to_quant, get_tvfeed_instance
from lib.pivots import getHHIndex, getHLIndex, getLLIndex, getLHIndex

import json

report = {}
def get_report_handle():
    global report
    return report

def get_dataframe(stock, market, timeframe, date, online):
    duration = 90
    if online or timeframe.strip().lower()[-1] not in ['d', 'w', 'm']:
        #Either we're explicitly told to fetch data from TV, or timeframe is shorter than a day
        username = 'AnshulBot'
        password = '@nshulthakur123'

        tv = get_tvfeed_instance(username, password)
        interval=convert_timeframe_to_quant(timeframe)

        symbol = stock.symbol.strip().replace('&', '_')
        symbol = symbol.replace('-', '_')
        nse_map = {'UNITDSPR': 'MCDOWELL_N',
                    'MOTHERSUMI': 'MSUMI'}
        if symbol in nse_map:
            symbol = nse_map[symbol]
        s_df = tv.get_hist(
                            symbol,
                            market.name,
                            interval=interval,
                            n_bars=duration,
                            extended_session=False,
                        )
        if len(s_df)==0:
            print('Skip {}'.format(symbol))
            pass
    else:
        s_df = get_stock_listing(stock, duration=duration, last_date = date)
        s_df = s_df.drop(columns = ['traded', 'trades', 'deliverable'])
        if len(s_df)==0:
            print('Skip {}'.format(stock.symbol))
    return s_df

def get_trend(stock, timeframe, date, online=False):
    df = get_dataframe(stock, timeframe, date, online)

    pass

def main(stock_name=None, exchange = None, timeframe= '15m', date=None, category = 'all', online=False):
    market = None
    if exchange is not None:
        try:
            market = Market.objects.get(name=exchange)
        except Market.DoesNotExist:
            print(f"No object exists for {exchange}")
            return
        if stock_name is None:
            for stock in Stock.objects.filter(market=market):
                try:
                    get_trend(stock, market, timeframe, date, online)
                except:
                    pass
            pass
        else:
            try:
                stock = Stock.objects.get(symbol=stock_name, market=market)
                get_trend(stock, market, timeframe, date, online)
            except Stock.DoesNotExist:
                print(f"Stock with symbol {stock_name} not found in {exchange}")
                return
    else:
        if stock_name is None:
            for stock in Stock.objects.all():
                try:
                    get_trend(stock, market, timeframe, date, online)
                except:
                    pass
        else:
            print('Also specify listing exchange for security')
            return
            
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Scan stock securities for trend')
    parser.add_argument('-s', '--stock', help="Stock code")
    parser.add_argument('-e', '--exchange', help="Exchange")
    parser.add_argument('-t', '--timeframe', help="Timeframe")
    parser.add_argument('-d', '--date', help="Date")
    parser.add_argument('-c', '--category', help="Category:One of 'all', 'fno', 'margin', 'portfolio'")
    parser.add_argument('-o', '--online', action='store_true', default=False, help="Online mode:Fetch from tradingview")
    args = parser.parse_args()
    stock_code = None
    day =  datetime.datetime.now()
    exchange = 'NSE'
    timeframe = '1h'
    category = 'all'
    
    if args.stock is not None and len(args.stock)>0:
        print('Scan data for stock {}'.format(args.stock))
        stock_code = args.stock
    if args.date is not None and len(args.date)>0:
        print('Scan data for date: {}'.format(args.date))
        try:
            day = datetime.datetime.strptime(args.date, "%d/%m/%y %H:%M")
        except:
            try:
                day = datetime.datetime.strptime(args.date, "%d/%m/%y")
            except:
                print('Error parsing date')
                day = None
    if args.exchange is not None and len(args.exchange)>0:
        exchange=args.exchange
    if args.timeframe is not None and len(args.timeframe)>0:
        timeframe=args.timeframe
    if args.category is not None:
        category = args.category
    main(stock_code, exchange, timeframe = timeframe, category=category, date=day, online=args.online)
