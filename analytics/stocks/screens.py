'''
Created on 19-May-2022

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

def rsi_scan(listing):
    results = []
    if listing.rsi[-1] < 30 and listing.rsi[-1] > 20:
        results.append('Overbought')
    elif listing.rsi[-1] <= 20:
        results.append('Severely Overbought')
    elif listing.rsi[-1] > 70 and listing.rsi[-1] < 80:
        results.append('Oversold')
    elif listing.rsi[-1] >= 80:
        results.append('Severely Oversold')
    
    return results

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
        dateval = dateval - datetime.timedelta(days=abs(4-datetime.date.today().weekday()))
    #if listing.index[-1].date() != dateval.date():
    if listing.index[-1].date() != dateval.date():
        print('No data for {} on stock {}. Last date: {}'.format(dateval, stock, listing.index[-1].date()))
        return
    try:
        result = rsi_scan(listing) 
        if len(result) > 0:
            print(f'{prefix}{stock.sid}:\n{result}')
    except:
        print(f'Exception occured in stock: {stock.sid}')
        traceback.print_exc()

def main(stock_name=None, date=None, category = ''):
    #List of FnO stocks (where bullish and bearish signals are required)
    fno = []
    with open('fno.txt', 'r') as fd:
        for line in fd:
            fno.append(line.strip())
    fno = sorted(fno)
    
    portfolio = []
    margin_stocks = []
    if category != 'fno':
        with open('portfolio.txt', 'r') as fd:
            for line in fd:
                portfolio.append(line.strip())
        portfolio = sorted(portfolio)
        
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
        if category != 'fno':
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
    parser.add_argument('-f', '--fno', help="Scan FnO stocks only", action='store_true', default=False)
    args = parser.parse_args()
    stock_code = None
    day = None
    
    category = 'all'
    
    if args.stock is not None and len(args.stock)>0:
        print('Scan data for stock {}'.format(args.stock))
        stock_code = args.stock
    if args.date is not None and len(args.date)>0:
        print('Scan data for date: {}'.format(args.date))
        day = datetime.datetime.strptime(args.date, "%d/%m/%y")
    if args.fno is True:
        category = 'fno'
    
    print('Scan for {}'.format(category))
    main(stock_code, day, category=category)
