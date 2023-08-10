'''
Created on 12-Apr-2022

@author: anshul
'''
import os
import sys
import matplotlib
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

from lib.retrieval import get_stock_listing
from lib.patterns import detect_fractals, get_volume_signals

import datetime

def evaluate_signal(df, signal_val, signal_frequency, success_frequency, tolerance=2):
    '''
    For the signal value passed, determine if price moves in the direction of signal 
    or not within the tolerance period and update the probability of success metric (frequentist)
    '''
    occurances = df[df[signal_val]!=0]
    #print(occurances.tail())
    idxs = []
    for idx, occurance in occurances.iterrows():
        #print(df.index.get_loc(idx))
        idxs.append(df.index.get_loc(idx))

    for idx in idxs:
        #print(idx)
        if df.iloc[idx][signal_val] > 0:
            #Bullish
            signal_frequency['bullish'][signal_val] += 1
            if idx+tolerance <= len(df)-1 and df.iloc[idx+tolerance]['close']> df.iloc[idx]['close']:
                #Pattern is success
                success_frequency['bullish'][signal_val] +=1
        elif df.iloc[idx][signal_val] < 0:
            #Bearish
            signal_frequency['bearish'][signal_val] += 1
            if idx+tolerance <= len(df)-1 and df.iloc[idx+tolerance]['close']< df.iloc[idx]['close']:
                #Pattern is success
                success_frequency['bearish'][signal_val] +=1


#For all stocks, for all days, run the above function once to determine the score of each indicator
# create columns for each pattern
candle_names = talib.get_function_groups()['Pattern Recognition']

signal_frequency = {'bullish': {}, 'bearish':{}}
success_frequency = {'bullish': {}, 'bearish':{}}

for candle in candle_names:
    signal_frequency['bullish'][candle] = 0
    success_frequency['bullish'][candle] = 0
    signal_frequency['bearish'][candle] = 0
    success_frequency['bearish'][candle] = 0
    
#Asian paints
#stock = Stock.objects.get(sid='ASIANPAINT')
#listing = get_stock_listing(stock, duration=-1, last_date = datetime.date.today())
#for candle in candle_names:
#    listing[candle] = getattr(talib, candle)(listing['open'], listing['high'], listing['low'], listing['close'])
#    evaluate_signal(listing, candle, signal_frequency, success_frequency)
    
for stock in Stock.objects.all():
    #listing = get_stock_listing(stock, duration=-1, last_date = datetime.date.today(), resample=False)
    listing = get_stock_listing(stock, duration=-1, last_date = datetime.date.today(), resample=False, monthly=False) #For weekly/monthly charts
    if len(listing)==0:
        continue
    for candle in candle_names:
        listing[candle] = getattr(talib, candle)(listing['open'], listing['high'], listing['low'], listing['close'])
        evaluate_signal(listing, candle, signal_frequency, success_frequency, tolerance=2)
        
print('Bullish')
for key in success_frequency['bullish']:
    if signal_frequency['bullish'][key] !=0:
        print(f"{key}: {success_frequency['bullish'][key]/signal_frequency['bullish'][key]}")
    else:
        print(f'{key}: NA')
print('Bearish')
for key in success_frequency['bearish']:
    if signal_frequency['bearish'][key] !=0:
        print(f"{key}: {success_frequency['bearish'][key]/signal_frequency['bearish'][key]}")
    else:
        print(f'{key}: NA')

#Compute efficacy of fractals patterns as well

signal_frequency = {'bullish': {}, 'bearish':{}}
success_frequency = {'bullish': {}, 'bearish':{}}

candle_names = ['fractal']
for candle in candle_names:
    signal_frequency['bullish'][candle] = 0
    success_frequency['bullish'][candle] = 0
    signal_frequency['bearish'][candle] = 0
    success_frequency['bearish'][candle] = 0
    
    
for stock in Stock.objects.all():
    #listing = get_stock_listing(stock, duration=-1, last_date = datetime.date.today(), resample=False)
    listing = get_stock_listing(stock, duration=-1, last_date = datetime.date.today(), resample=False, monthly=False) #For weekly/monthly charts
    if len(listing)==0:
        continue
    for candle in candle_names:
        df = detect_fractals(listing)
        evaluate_signal(listing, candle, signal_frequency, success_frequency, tolerance=3)
        
print('Bullish')
for key in success_frequency['bullish']:
    if signal_frequency['bullish'][key] !=0:
        print(f"{key}: {success_frequency['bullish'][key]/signal_frequency['bullish'][key]}")
    else:
        print(f'{key}: NA')
print('Bearish')
for key in success_frequency['bearish']:
    if signal_frequency['bearish'][key] !=0:
        print(f"{key}: {success_frequency['bearish'][key]/signal_frequency['bearish'][key]}")
    else:
        print(f'{key}: NA')
        

print(signal_frequency['bullish']['fractal'])
print(success_frequency['bullish']['fractal'])

print(signal_frequency['bearish']['fractal'])
print(success_frequency['bearish']['fractal'])

