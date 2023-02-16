#Consolidate all file contents with the same name 

import os
import sys
import settings
import pandas as pd

from lib.indices import get_index_members

members = get_index_members('NIFTY 50')
year = 2023
month = 'JAN'

path = f'intraday/{year}/{month}/'

for member in members:
    files = []
    for day in range(1,32):
        if os.path.isfile(f'{path}/{day:02d}{month}/{day:02d}{month}/{member}.txt'):
            files.append(f'{path}/{day:02d}{month}/{day:02d}{month}/{member}.txt')
    df = pd.read_csv(files.pop(0))
    for f in files:
        s_df = pd.read_csv(f)
        df.merge(s_df, on='title')

    df.to_csv(f'{path}/{member}.csv', index=False)
# a = pd.read_csv(f"{path}/")
# b = pd.read_csv("fileb.csv")
# b = b.dropna(axis=1)
# merged = a.merge(b, on='title')
# merged.to_csv("output.csv", index=False)