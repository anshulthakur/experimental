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

import datetime
from stocks.models import Listing, Stock

from lib.retrieval import get_stock_listing
from lib.patterns import detect_fractals, get_volume_signals, get_last_day_patterns, get_signals

def do_stuff(stock, prefix=''):
    listing = get_stock_listing(stock, duration=30, last_date = datetime.date.today(), 
                                studies={'daily': ['rsi', 'ema20', 'sma200'],})
                                         #'weekly':['rsi', 'ema20', 'ema10'],
                                         #'monthly': ['rsi']
                                        #})
    #print(listing.tail())
    if len(listing)==0:
        return
    #if listing.index[-1].date() != datetime.date.today() - datetime.timedelta(days=1):
    dateval = datetime.date.today()
    if datetime.date.today().weekday() in [5,6]: #If its saturday (5) or sunday (6)
        dateval = dateval - datetime.timedelta(days=abs(4-datetime.date.today()))
    if listing.index[-1].date() != dateval:
        print('No data for today on stock {}'.format(stock))
        return
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
    for key in vol_sigs:
        if vol_sigs[key] is not None:
            print(f"{key}: {vol_sigs[key]}")

#List of FnO stocks (where bullish and bearish signals are required)
fno = []
with open('fno.txt', 'r') as fd:
    for line in fd:
        fno.append(line.strip())
fno = sorted(fno)
for sname in fno:
    try:
        stock = Stock.objects.get(sid=sname)
        do_stuff(stock, prefix='[FnO]')
    except Stock.DoesNotExist:
        print(f'{sname} name not present')

        
portfolio = []
with open('portfolio.txt', 'r') as fd:
    for line in fd:
        portfolio.append(line.strip())
portfolio = sorted(portfolio)
for sname in portfolio:
    try:
        stock = Stock.objects.get(sid=sname)
        do_stuff(stock, prefix='[PF]')
    except Stock.DoesNotExist:
        print(f'{sname} name not present')
        
margin_stocks = []
with open('margin.txt', 'r') as fd:
    for line in fd:
        margin_stocks.append(line.strip())
        
for stock in Stock.objects.all():
    #listing = get_stock_listing(stock, duration=30, last_date = datetime.date(2020, 12, 31))
    #print(stock)
    if (stock.sid in fno) or (stock.sid in portfolio):
        continue
    if stock.sid in margin_stocks:
        do_stuff(stock, prefix='[MG]')
    else:
        do_stuff(stock)