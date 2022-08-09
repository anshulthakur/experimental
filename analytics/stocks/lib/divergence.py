'''
Created on 30-Jul-2022

@author: anshul
'''
import numpy as np
import talib as ta
import datetime

from lib.pivots import getPeaks, getValleys

def detect_divergence(df=None, indicator='RSI', after = datetime.datetime.today().strftime('%Y-%m-%d 00:00:00')):
    method = ta.RSI
    
    df[indicator.lower()] = method(df.close, timeperiod=14)
    df.dropna(inplace=True)
    
    order = 1
    close_highs = getPeaks(df, key='close', order=order)
    close_lows = getValleys(df, key='close', order=order)
    
    ind_highs = getPeaks(df, key=indicator.lower(), order=order)
    ind_lows = getValleys(df, key=indicator.lower(), order=order)
    
    def get_divergence(df, index):
        if close_lows[index] == 1 and (ind_lows[index] == -1):
            return 1
        elif close_highs[index] == 1 and (ind_highs[index] == -1):
            return -1
    df['divergence'] = df.apply(lambda x: get_divergence(df, df.index.get_loc(x.name)), axis=1)
    pos = df[df['divergence']==1]
    neg = df[df['divergence']==-1]
    
    if not pos.empty:
        if after is not None:
            pos = pos.loc[after:]
            if not pos.empty:
                #print('Positive Divergences')
                #print(pos.tail())
                pass
        else:
            #print('Positive Divergences')
            #print(pos.tail())
            pass
    
    if not neg.empty:
        if after is not None:
            neg = neg.loc[after:]
            if not neg.empty:
                #print('Negative Divergences')
                #print(neg.tail())
                pass
            #print(neg.count())
        else:
            #print('Negative Divergences')
            #print(neg.tail())
            pass
    return [pos, neg]