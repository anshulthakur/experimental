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
from lib.logging import set_loglevel, log
import json

report = {}
def get_report_handle():
    global report
    return report

def get_dataframe(stock, market, timeframe, date, online=False):
    duration = 60
    if 'w' in timeframe.lower():
        duration = duration * 5 
    if 'm' in timeframe.lower():
        duration = duration * 25

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
            log('Skip {}'.format(symbol), logtype='warning')
            pass
    else:
        s_df = get_stock_listing(stock, duration=duration, last_date = date, 
                                    resample=True if 'w' in timeframe.lower() else False, 
                                    monthly=True if 'm' in timeframe.lower() else False)
        s_df = s_df.drop(columns = ['volume', 'delivery', 'trades'])
        if len(s_df)==0:
            log('Skip {}'.format(stock.symbol), logtype='warning')
    return s_df

def get_trend(stock, market, timeframe, date, online=False):
    report = get_report_handle()
    df = get_dataframe(stock, market, timeframe, date, online)

    hh_val = 0
    hl_val = 0
    lh_val = 0
    ll_val = 0
    order = 1

    hh = getHHIndex(df.close.values, order=order)
    if hh is not None and len(hh)>1:
        hh = hh[-1]-order
        hh_val = df.close[hh]
    else:
        hh = -1
    hl = getHLIndex(df.close.values, order=order)
    if hl is not None and len(hl)>1:
        hl = hl[-1]-order
        hl_val = df.close[hl]
    else:
        hl = -1
    ll = getLLIndex(df.close.values, order=order)
    if ll is not None and len(ll)>1:
        ll = ll[-1]-order
        ll_val = df.close[ll]
    else:
        ll = -1
    lh = getLHIndex(df.close.values, order=order)
    if lh is not None and len(lh)>1:
        lh = lh[-1]-order
        lh_val = df.close[lh]
    else:
        lh = -1
    log(f'\n{stock.symbol}: LL: {ll} LH: {lh} HH: {hh} HL: {hl}', logtype='debug')
    log(df.head(10), logtype='debug')
    if hh>ll and hl>ll:
        if hh>hl:
            report[stock.symbol] = 'Uptrend'
            log(f'{stock.symbol}: Uptrend', logtype='debug')
        else:
            if hl > hl and df.close[-1] > hl_val:
                report[stock.symbol] = 'Uptrend (pending confirmation)'
                log(f'{stock.symbol}: Uptrend (pending confirmation)', logtype='debug')
    if lh > hh and lh > hl:
        if ll > lh:
            report[stock.symbol] = 'Downtrend'
            log(f'{stock.symbol}: Downtrend', logtype='debug')
        else:
            if lh > ll and df.close[-1] < ll_val:
                report[stock.symbol] = 'Downtrend (pending confirmation)'
                log(f'{stock.symbol}: Downtrend (pending confirmation)', logtype='debug')


def main(stock_name=None, exchange = None, timeframe= '1d', date=None, online=False):
    market = None
    if exchange is not None:
        try:
            market = Market.objects.get(name=exchange)
        except Market.DoesNotExist:
            log(f"No object exists for {exchange}", logtype='error')
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
                log(f"Stock with symbol {stock_name} not found in {exchange}", logtype='error')
                return
    else:
        if stock_name is None:
            for stock in Stock.objects.all():
                try:
                    get_trend(stock, market, timeframe, date, online)
                except:
                    pass
        else:
            log('Also specify listing exchange for security', logtype='error')
            return

if __name__ == "__main__":
    set_loglevel('info')
    import argparse
    parser = argparse.ArgumentParser(description='Scan stock securities for trend')
    parser.add_argument('-s', '--stock', help="Stock code")
    parser.add_argument('-e', '--exchange', help="Exchange")
    parser.add_argument('-t', '--timeframe', help="Timeframe")
    parser.add_argument('-d', '--date', help="Date")
    parser.add_argument('-l', '--list', help="List of stocks to scan (MARKET:SYMBOL)")
    parser.add_argument('-o', '--online', action='store_true', default=False, help="Online mode:Fetch from tradingview")
    args = parser.parse_args()
    stock_code = None
    day =  datetime.datetime.now()
    exchange = 'NSE'
    timeframe = '1d'
    category = 'all'
    
    if args.stock is not None and len(args.stock)>0:
        log('Scan data for stock {}'.format(args.stock), logtype='info')
        stock_code = args.stock
    if args.date is not None and len(args.date)>0:
        log('Scan data for date: {}'.format(args.date), logtype='info')
        try:
            day = datetime.datetime.strptime(args.date, "%d/%m/%y %H:%M")
        except:
            try:
                day = datetime.datetime.strptime(args.date, "%d/%m/%y")
            except:
                log('Error parsing date', logtype='error')
                day = None
    if args.exchange is not None and len(args.exchange)>0:
        exchange=args.exchange
    if args.timeframe is not None and len(args.timeframe)>0:
        timeframe=args.timeframe
    
    if args.list is None:
        main(stock_code, exchange, timeframe = timeframe, date=day, online=args.online)
    else:
        stock_list = []
        with open(args.list, 'r') as fd:
            for line in fd:
                stock_list.append(line.strip().upper())
        for stock in stock_list:
            s = stock.split(':')[1]
            m = stock.split(':')[0]
            main(s, m, timeframe = timeframe, date=day, online=args.online)
    
    report = get_report_handle()
    log(json.dumps(report, indent=2), logtype='info')