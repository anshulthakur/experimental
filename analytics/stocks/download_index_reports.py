'''
Created on 26-Aug-2022

@author: Anshul
'''
import os
import sys
import csv
import datetime
import urllib
import requests

url_base_path = "https://www1.nseindia.com/content/indices/ind_close_all_{date}.csv" #25082022
download_path = './reports/daily/'
index_files_path = './reports/'

fields = ['Index Name', 'Index Date', 'Open Index Value', 
              'High Index Value', 'Low Index Value', 'Closing Index Value',
              'Points Change', 'Change(%)', 'Volume', 'Turnover (Rs. Cr.)',
              'P/E', 'P/B', 'Div Yield']
replacement_symbols = ['-', '/', ' ', ':', '(', ')', '%']

def download_daily_data(day, silent=False):
    import os.path
    
    filepath = download_path+'ind_close_all_{date}.csv'.format(date=day.strftime("%d%m%Y"))
    if os.path.isfile(filepath):
        if not silent:
            print(f'File: {filepath} exists. Skip download')
        return filepath
    url = url_base_path.format(date=day.strftime("%d%m%Y"))
    headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:57.0) Gecko/20100101 Firefox/57.0', 
            'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',
            }
    try:
        session = requests.Session()
        response = session.get(url, headers=headers)
        response.raise_for_status()
        open(filepath, 'wb').write(response.content)
    except Exception as e:
        print (f'ERR:: Exception occurred while fetching data foor day: {day}')
        if not silent:
            print(e)
        return False
    
    return filepath

def download_historical_data(day, silent=False):
    '''
    Will download past 52 weeks data. That's 52*5 calls
    '''
    
    files = []
    
    for delta in range(0, 365):
        download_date = day - datetime.timedelta(days=delta)
        if download_date.weekday() in [5,6]:
            continue
        f = download_daily_data(download_date, silent)
        if f is not False:
            files.append(f)
    reports = {}
    for report in files:
        with open(report) as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                index = row['Index Name'].strip()
                for s in replacement_symbols:
                    index = index.replace(s, '_')
                if index not in reports:
                    reports[index] = []
                reports[index].append(row)
    for index in reports:
        with open(index_files_path+index+'.csv', 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fields)
            writer.writeheader()
            for r in reports[index]:
                writer.writerow(r)

def update_daily_data(day, silent=False):
    filepath = download_path+'ind_close_all_{date}.csv'.format(date=day.strftime("%d%m%Y"))
    reports = {}
    
    with open(filepath, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            index = row['Index Name'].strip()
            for s in replacement_symbols:
                index = index.replace(s, '_')
            if index not in reports:
                reports[index] = []
            reports[index].append(row)
    for index in reports:
        with open(index_files_path+index+'.csv', 'a') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fields)
            for r in reports[index]:
                writer.writerow(r)

if __name__ == "__main__":
    day = datetime.date.today()
    import argparse
    parser = argparse.ArgumentParser(description='Download indices data')
    parser.add_argument('-d', '--date', help="Date")
    parser.add_argument('-a', '--all', action='store_true', default=False, help="Download all historical data")
    args = parser.parse_args()
    stock_code = None
    
    if args.date is not None and len(args.date)>0:
        print('Get data for date: {}'.format(args.date))
        day = datetime.datetime.strptime(args.date, "%d/%m/%y")
    
    if args.all:
        download_historical_data(day, silent=False)
        exit(0)
    
    download_daily_data(day)
    update_daily_data(day)
