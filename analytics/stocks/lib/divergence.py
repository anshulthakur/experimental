'''
Created on 30-Jul-2022

@author: anshul
'''
import numpy as np
import talib as ta
import datetime

from lib.pivots import getPeaks, getValleys

def get_divergence_points(df=None, indicator='RSI', after = datetime.datetime.today().strftime('%Y-%m-%d 00:00:00')):
    '''
    This method adds a column of the indicator and another column indicating its divergence. It works on the provided 
    dataframe. Also, it returns the indices of the positive and negative divergences as a result tuple (pos, neg).
    '''
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
            return 1 #Diverging with Price making LL and Indicator making HL: Positive Divergence
        elif close_highs[index] == 1 and (ind_highs[index] == -1):
            return 1 #Diverging with Price making HH and Indicator making LH: Negative Divergence 
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

def is_diverging(df=None, indicator='RSI', after = datetime.datetime.today().strftime('%Y-%m-%d 00:00:00')):
    method = ta.RSI
    
    ind_df = method(df.close, timeperiod=14)
    ind_df.dropna(inplace=True)
    
    order = 1
    close_highs = getPeaks(df, key='close', order=order)
    close_lows = getValleys(df, key='close', order=order)
    
    ind_highs = getPeaks(ind_df, key=indicator.lower(), order=order)
    ind_lows = getValleys(ind_df, key=indicator.lower(), order=order)
    
    if close_lows[-1] == 1 and (ind_lows[-1] == -1):
        return 1
    elif close_highs[-1] == 1 and (ind_highs[-1] == -1):
        return -1
    else:
        return 0

def detect_divergence(df=None, indicator='RSI', key = 'close', order=1):
    '''
    This method assumes that the indicator for which divergence is to be computed is already added to the DF.
    It returns a new dataframe that contains the divergence values (+1 for positive divergence, -1 for 
    negative, and 0 for no divergence)

    @param df Dataframe to work on
    @param indicator Indicator for which divergence is being computed
    @param key What field to use as a comparison with the indicator (one of OHLC)
    @param order The amount of lookaround to do while finding peaks and valleys
    '''
    close_highs = getPeaks(df, key=key, order=order)
    close_lows = getValleys(df, key=key, order=order)
    
    ind_highs = getPeaks(df, key=indicator, order=order)
    ind_lows = getValleys(df, key=indicator, order=order)
    
    def get_divergence(df, index):
        if close_lows[index] == 1 and (ind_lows[index] == -1):
            return 1 #Diverging with Price making LL and Indicator making HL: Positive Divergence
        elif close_highs[index] == 1 and (ind_highs[index] == -1):
            return 1 #Diverging with Price making HH and Indicator making LH: Negative Divergence 
        else:
            return 0
    return df.apply(lambda x: get_divergence(df, df.index.get_loc(x.name)), axis=1)