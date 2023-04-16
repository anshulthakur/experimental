import os
import sys
import settings

import numpy as np
import pandas as pd

from lib.misc import create_intraday_folders, get_filelist, create_directory
from lib.logging import log
raw_dir = './intraday_raw/'

def format_intraday(filename):
    log(filename)
    try:
        df_source = pd.read_csv(filename, names=['name', 'date', 'time', 'open', 'high', 'low', 'close'],
                                        dtype={'date': str,
                                            'time': str,
                                            'name': str,
                                            'open': float,
                                            'high': float,
                                            'low': float,
                                            'close': float},
                                        usecols=[0,1,2,3,4,5,6])
        df_source['datetime'] = pd.to_datetime(df_source['date'] + df_source['time'], format='%Y%m%d%H:%M')
        df_source.set_index('datetime', drop=True, inplace=True)
        df_source.drop(columns=['date', 'time'], inplace=True)
        df_source.reindex()
        df_source = df_source.sort_index()
        #print(df_source.head(10))
        #Slice the dataframe by scrip name
        result = [group[1] for group in df_source.groupby('name')]
        #Slice the dataframe into day-wise multiple dataframes
        for df in result:
            scrip = df['name'][0]
            df_list = [group[1] for group in df.groupby(df.index.date)]
            for s_df in df_list:
                day = s_df.index[0].date().day
                #print(day)
                directory = os.path.join(settings.project_dirs.get('intraday'), 
                                str(pd.to_datetime(s_df.index[0]).year), 
                                f'{pd.to_datetime(s_df.index[0]).month:02d}',
                                f'{day:02d}')
                f = os.path.join(directory,scrip+'.csv')
                create_directory(directory)
                if not os.path.exists(f):
                    s_df.to_csv(f)
                    log(f)
                #print(s_df.head(10))
    except:
        log(f"Error in processing {filename}")
    pass

if __name__ == "__main__":
    #create_intraday_folders(base_folder=settings.project_dirs.get('intraday'))
    filelist = get_filelist(raw_dir, recursive=True)
    for f in filelist:
        format_intraday(f)
        #break