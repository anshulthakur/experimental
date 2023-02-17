#Consolidate all file contents with the same name 

import os
import sys
import settings
import pandas as pd
import numpy as np
import glob

from lib.indices import get_index_members


year = 2023
month = 'JAN'

path = f'intraday/{year}/{month}/'

def get_symbols():
    dir_to_exclude = []

    files = glob.glob(f'{path}/**/*.txt', recursive=True)
    files_paths = [_ for _ in files if _.split("\\")[0] not in dir_to_exclude]
    files_names = [_.split("\\")[-1] for _ in files if _.split("\\")[0] not in dir_to_exclude]

    #print(f'List of file names with path: {files_paths}')
    #print(f'List of file names: {files_names}')
    symbols = set([(f.split('/')[-1]).split('.')[0] for f in files_names])
    s_map = {}
    for symbol in symbols:
        s_map[symbol] = []
        for f in files_paths:
            if symbol in f:
                s_map[symbol].append(f)
    
    return s_map

def consolidate(s_map):
    for symbol in s_map:
        print(symbol)
        files = s_map[symbol]
        df_arr = []
        for f in files:
            s_df = pd.read_csv(f, names=['symbol', 'date', 'time', 'open', 'high', 'low', 'close', 'volume', 'other'], 
                                dtype={
                                    'symbol': str, 
                                    'date': str, 
                                    'time': str, 
                                    'open': float, 
                                    'high': float, 
                                    'low': float, 
                                    'close': float, 
                                    'volume': np.int64, 
                                    'other': np.int64,
                                })
            #print(s_df.head(2))
            df_arr.append(s_df)
        df = pd.concat(df_arr, axis='rows', ignore_index=True)
            #df.merge(s_df, on='title')

        #print([col for col in df.columns])
        #Merge date and time to a datetime column
        df['datetime'] = pd.to_datetime(df['date'] +df['time'], format='%Y%m%d%H:%M')
        df.drop(columns=['date', 'time', 'symbol'], inplace=True)
        df.set_index('datetime', inplace=True)
        df.reset_index(inplace=True)
        df.drop_duplicates(inplace=True)
        df.sort_index(inplace=True)
        
        df.to_csv(f'{path}/{symbol}.csv', index=False)


def consolidate_index():
    members = get_index_members('NIFTY 50')
    for member in members:
        print(member)
        files = []
        for day in range(1,32):
            if os.path.isfile(f'{path}/{day:02d}{month}/{day:02d}{month}/{member}.txt'):
                files.append(f'{path}/{day:02d}{month}/{day:02d}{month}/{member}.txt')
        #df = pd.read_csv(files.pop(0), names=['symbol', 'date', 'time', 'open', 'high', 'low', 'close', 'volume', 'other'])
        df_arr = []
        for f in files:
            s_df = pd.read_csv(f, names=['symbol', 'date', 'time', 'open', 'high', 'low', 'close', 'volume', 'other'], 
                                dtype={
                                    'symbol': str, 
                                    'date': str, 
                                    'time': str, 
                                    'open': float, 
                                    'high': float, 
                                    'low': float, 
                                    'close': float, 
                                    'volume': np.int64, 
                                    'other': np.int64,
                                })
            #print(s_df.head(2))
            df_arr.append(s_df)
        df = pd.concat(df_arr, axis='rows', ignore_index=True)
            #df.merge(s_df, on='title')

        #print([col for col in df.columns])
        #Merge date and time to a datetime column
        df['datetime'] = pd.to_datetime(df['date'] +df['time'], format='%Y%m%d%H:%M')
        df.drop(columns=['date', 'time', 'symbol'], inplace=True)
        df.set_index('datetime', inplace=True)
        df.reset_index(inplace=True)
        df.drop_duplicates(inplace=True)
        df.sort_index(inplace=True)
        
        df.to_csv(f'{path}/{member}.csv', index=False)
        #break
consolidate(get_symbols())