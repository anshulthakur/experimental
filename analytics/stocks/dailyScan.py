'''
Created on 12-Apr-2022

@author: anshul
'''
import os
import sys
import matplotlib
import settings
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

import traceback

import datetime
from stocks.models import Listing, Stock

from lib.retrieval import get_stock_listing
from lib.patterns import detect_fractals, get_volume_signals, get_last_day_patterns, get_signals

def do_stuff(stock, prefix='', date = None):
    if date is None:
        date = datetime.date.today()
    listing = get_stock_listing(stock, duration=30, last_date = date, 
                                studies={'daily': ['rsi', 'ema20', 'sma200'],})
                                         #'weekly':['rsi', 'ema20', 'ema10'],
                                         #'monthly': ['rsi']
                                        #})
    #print(listing.tail())
    if len(listing)==0:
        return
    #if listing.index[-1].date() != datetime.date.today() - datetime.timedelta(days=1):
    dateval = date
    if datetime.date.today().weekday() in [5,6]: #If its saturday (5) or sunday (6)
        dateval = dateval - datetime.timedelta(days=abs(4-datetime.date.today()))
    #if listing.index[-1].date() != dateval.date():
    if listing.index[-1].date() != dateval:
        print('No data for {} on stock {}. Last date: {}'.format(dateval, stock, listing.index[-1].date()))
        return
    try:
        patterns = get_last_day_patterns(listing)
        signals = get_signals(listing)
        vol_sigs = get_volume_signals(listing)
        if len(patterns['bullish'])>0 or len(patterns['bearish'])>0:
            print(f"{prefix}{stock.sid} \nBullish: {patterns['bullish']}\tBearish: {patterns['bearish']}")
        else:
            print(f"{prefix}{stock.sid}: No patterns")
        if len(signals['proximity_short'])>0:
            print(f"Bearish Proximity signals: {signals['proximity_short']}")
        if len(signals['proximity_long'])>0:
            print(f"Bullish Proximity signals: {signals['proximity_long']}")
        if len(signals['price_crossover_short'])>0:
            print(f"Bearish Crossover signals: {signals['price_crossover_short']}")
        if len(signals['price_crossover_long'])>0:
            print(f"Bullish Crossover signals: {signals['price_crossover_long']}")
        for key in signals:
            if key in ['volatility_expand', 'volatility_contract'] and signals[key] is not None:
                print(f"{key}: {signals[key]}")
        for key in vol_sigs:
            if vol_sigs[key] is not None:
                print(f"{key}: {vol_sigs[key]}")
    except:
        print(f'Exception occured in stock: {stock.sid}')
        traceback.print_exc()

def main(stock_name=None, date=None):
    #List of FnO stocks (where bullish and bearish signals are required)
    fno = []
    with open('fno.txt', 'r') as fd:
        for line in fd:
            fno.append(line.strip())
    fno = sorted(fno)
    
    portfolio = []
    with open('portfolio.txt', 'r') as fd:
        for line in fd:
            portfolio.append(line.strip())
    portfolio = sorted(portfolio)
    
    margin_stocks = []
    with open('margin.txt', 'r') as fd:
        for line in fd:
            margin_stocks.append(line.strip())
            
    if stock_name is None:
        for sname in fno:
            try:
                stock = Stock.objects.get(sid=sname)
                do_stuff(stock, prefix='[FnO]', date=date)
            except Stock.DoesNotExist:
                print(f'{sname} name not present')
        for sname in portfolio:
            try:
                stock = Stock.objects.get(sid=sname)
                do_stuff(stock, prefix='[PF]', date=date)
            except Stock.DoesNotExist:
                print(f'{sname} name not present')
        for stock in Stock.objects.all():
            #listing = get_stock_listing(stock, duration=30, last_date = datetime.date(2020, 12, 31))
            #print(stock)
            if (stock.sid in fno) or (stock.sid in portfolio):
                continue
            if stock.sid in margin_stocks:
                do_stuff(stock, prefix='[MG]', date=date)
            else:
                do_stuff(stock, date=date)
    else:
        prefix = ''
        stock = Stock.objects.get(sid=stock_name)
        if stock in fno:
            prefix='[FnO]'
        elif stock in portfolio:
            prefix='[PF]'
        elif stock in margin_stocks:
            prefix='[MG]'
        do_stuff(stock, prefix=prefix, date=date)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Scan stock info')
    parser.add_argument('-s', '--stock', help="Stock code")
    parser.add_argument('-d', '--date', help="Date")
    args = parser.parse_args()
    stock_code = None
    day = None
    
    if args.stock is not None and len(args.stock)>0:
        print('Scan data for stock {}'.format(args.stock))
        stock_code = args.stock
    if args.date is not None and len(args.date)>0:
        print('Scan data for date: {}'.format(args.date))
        day = datetime.datetime.strptime(args.date, "%d/%m/%y")
    
    main(stock_code, day)