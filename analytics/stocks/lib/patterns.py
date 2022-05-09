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

import talib
from talib.abstract import *
from talib import MA_Type


def detect_fractals(df):
    df['fractal'] = 0
    
    for i in range(2,df.shape[0]-2):
        if df['low'][i] < df['low'][i-1]  and df['low'][i] < df['low'][i+1] and df['low'][i+1] < df['low'][i+2] and df['low'][i-1] < df['low'][i-2]:
            #df['fractal'][i] = 1.0
            df.iloc[i, df.columns.get_loc('fractal')] = 1.0
        elif df['high'][i] > df['high'][i-1]  and df['high'][i] > df['high'][i+1] and df['high'][i+1] > df['high'][i+2] and df['high'][i-1] > df['high'][i-2]:
            #df['fractal'][i] = -1.0
            df.iat[i, df.columns.get_loc('fractal')] = -1.0
    return df

def get_volume_signals(df):
    '''
    Look at the trading volume and delivery volumes
    1. Check if unusually large amount of delivery volume is there (not percentages)
    2. Check if unusually large trading volume is there
    '''
    signals = {'volume_shoot_up': None,
               'volume_contract': None,
               'delivery_shoot_up': None,
               'delivery_contract': None,
               'delivery_ptage_shoot_up': None,
               'delivery_ptage_contract': None,
               'volume_per_trade_shoot_up': None,
               'volume_per_trade_contract': None,}
    period = 20 #20 day mean
    std = df['volume'].ewm(span=period).std()
    
    #std = df.iloc[-21:-1]['volume'].std()
    vol_ema = talib.EMA(df['volume'], period)

    if np.isnan(vol_ema.iloc[-2])==False and  std.iloc[-2] != 0 and df.iloc[-1]['volume'] >= (vol_ema.iloc[-2]+ std.iloc[-2]):
        signals['volume_shoot_up'] = (df.iloc[-1]['volume'] - vol_ema.iloc[-2])/std.iloc[-2]
    elif np.isnan(vol_ema.iloc[-2])==False and std.iloc[-2] != 0 and df.iloc[-1]['volume'] <= (vol_ema.iloc[-2]- std.iloc[-2]):
        signals['volume_contract'] = (vol_ema.iloc[-2] - df.iloc[-1]['volume'])/std.iloc[-2]
    #else:
    #    print(f"Volume: {df.iloc[-1]['volume']}  EMA: {vol_ema.iloc[-2]} STD: {std}")
        
    #std = df[-21:-1]['delivery'].std()
    std = df['delivery'].ewm(span=period).std()
    del_ema = talib.EMA(df['delivery'], period)
    if np.isnan(del_ema.iloc[-2])==False and std.iloc[-2] != 0 and df.iloc[-1]['delivery'] >= (del_ema.iloc[-2]+ std.iloc[-2]):
        signals['delivery_shoot_up'] = (df.iloc[-1]['delivery'] - del_ema.iloc[-2])/std.iloc[-2]
    elif np.isnan(del_ema.iloc[-2])==False and std.iloc[-2] != 0 and df.iloc[-1]['delivery'] <= (del_ema.iloc[-2]- std.iloc[-2]):
        signals['delivery_contract'] = (del_ema.iloc[-2] - df.iloc[-1]['delivery'])/std.iloc[-2]
    #else:
    #    print(f"Delivery: {df.iloc[-1]['delivery']}  EMA: {del_ema.iloc[-2]} STD: {std}")
    
    dl_ptage = df['delivery']/df['volume']
    dl_ema = talib.SMA(dl_ptage, period)
    #std = dl_ptage[-21:-1].std()
    std = dl_ptage.ewm(span=period).std()
    if np.isnan(dl_ema.iloc[-2])==False and std.iloc[-2] != 0 and dl_ptage.iloc[-1] >= (dl_ema.iloc[-2]+ std.iloc[-2]):
        signals['delivery_ptage_shoot_up'] = (dl_ptage.iloc[-1] - dl_ema.iloc[-2])/std.iloc[-2]
    elif np.isnan(dl_ema.iloc[-2])==False and std.iloc[-2] != 0 and dl_ptage.iloc[-1] <= (dl_ema.iloc[-2]- std.iloc[-2]):
        signals['delivery_ptage_contract'] = (dl_ema.iloc[-2] - dl_ptage.iloc[-1])/std.iloc[-2]
    #else:
    #    print(f"Delivery %: {dl_ptage.iloc[-1]}  EMA: {dl_ema.iloc[-2]} STD: {std}")
    
    vpt_ptage = df['volume']/df['trades'] #Volume per trade
    dl_ema = talib.SMA(vpt_ptage, period)
    #std = vpt_ptage[-21:-1].std()
    std = vpt_ptage.ewm(span=period).std()
    if np.isnan(dl_ema.iloc[-2])==False and std.iloc[-2] != 0 and vpt_ptage.iloc[-1] >= (dl_ema.iloc[-2]+ std.iloc[-2]):
        signals['volume_per_trade_shoot_up'] = (vpt_ptage.iloc[-1] - dl_ema.iloc[-2])/std.iloc[-2]
    elif np.isnan(dl_ema.iloc[-2])==False and std.iloc[-2] != 0 and vpt_ptage.iloc[-1] <= (dl_ema.iloc[-2]- std.iloc[-2]):
        signals['volume_per_trade_contract'] = (dl_ema.iloc[-2] - vpt_ptage.iloc[-1])/std.iloc[-2]
    #else:
    #    print(f"Volume Per trade %: {vpt_ptage.iloc[-1]}  EMA: {dl_ema.iloc[-2]} STD: {std}")
     
    
    return signals

def get_last_day_patterns(df):
    candle_names = talib.get_function_groups()['Pattern Recognition']
    patterns = {'bullish': [], 'bearish': []}
    for candle in candle_names:
        df[candle] = getattr(talib, candle)(df['open'], df['high'], df['low'], df['close'])
        if df.iloc[-1][candle]>0:
            patterns['bullish'].append(candle)
        elif df.iloc[-1][candle]<0:
            patterns['bearish'].append(candle)
    df = detect_fractals(df)
    if df.iloc[-3]['fractal']>0:
        patterns['bullish'].append('fractal')
    elif df.iloc[-3]['fractal']<0:
        patterns['bearish'].append('fractal')
    #print(df.iloc[-1])
    return patterns

def get_signals(df, threshold = 0.05):
    overlap_indicators = ['ema20', 'ema10', 'sma200',
                  'w_ema20', 'w_ema10', 'w_sma200',]
    signals = {'proximity_short': [],
               'proximity_long': [],
               'price_crossover_short': [],
               'price_crossover_long': [],
               'volatility_contract': None,
               'volatility_expand': None}
    for indicator in overlap_indicators:
        if indicator in df.columns:
            #If price is near the indicator (5%), flag the indicator
            if (abs(df.iloc[-1]['close'] - df.iloc[-1][indicator])/df.iloc[-1]['close'])<= threshold:
                if df.iloc[-1]['close'] < df.iloc[-1][indicator]:
                    signals['proximity_short'].append(indicator)
                if df.iloc[-1]['close'] > df.iloc[-1][indicator]:
                    signals['proximity_long'].append(indicator)
            #If price is crossing over the indicator, then flag too
            if df.iloc[-1]['low'] < df.iloc[-1][indicator] and df.iloc[-1]['high'] >= df.iloc[-1][indicator]:
                if df.iloc[-1]['close'] > df.iloc[-1]['open']:
                    signals['price_crossover_short'].append(indicator)
                else:
                    signals['price_crossover_long'].append(indicator)
    iv = df['high'] - df['low']
    period = 20 #20 day mean
    std = iv.ewm(span=period).std()
    
    iv_ema = talib.EMA(iv, period)
    if np.isnan(iv_ema.iloc[-2])==False and  std.iloc[-2] != 0 and iv.iloc[-1] >= (iv_ema.iloc[-2]+ std.iloc[-2]):
        signals['volatility_expand'] = (iv.iloc[-1] - iv_ema.iloc[-2])/std.iloc[-2]
    elif np.isnan(iv_ema.iloc[-2])==False and std.iloc[-2] != 0 and iv.iloc[-1] <= (iv_ema.iloc[-2]- std.iloc[-2]):
        signals['volatility_contract'] = (iv_ema.iloc[-2] - iv.iloc[-1])/std.iloc[-2]
    return signals