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

from lib.tradingview import TvDatafeed, Interval, convert_timeframe_to_quant
from lib.divergence import detect_divergence

import json


def get_report_dict(symbol=None):
    return {'symbol': symbol,
           'time': None,
           'rsi': None,
           'divergence': None,
           'region': None,
           'cmp': None,
           'price': None}

def get_field_names():
    return list(get_report_dict().keys())

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

def get_rsi_region(rsi):
    results = ''
    if rsi < 30 and rsi > 20:
        results = 'Overbought'
    elif rsi <= 20:
        results = 'Severely Overbought'
    elif rsi > 70 and rsi < 80:
        results = 'Oversold'
    elif rsi >= 80:
        results = 'Severely Oversold'
        
    return results

def do_stuff(stock, prefix='', date = None):
    if date is None:
        date = datetime.datetime.now()
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


def divergence_scan(symbol=None, exchange = 'NSE', timeframe= convert_timeframe_to_quant('1h'), after=None):
    username = 'AnshulBot'
    password = '@nshulthakur123'
    
    tv = TvDatafeed(username, password)
    
    interval = timeframe
    
    
    #n_bars = int((14.5)*60/interval)+14
    n_bars = 150
    #if exchange!='MCX':
    #    n_bars = int((6.25)*60/interval)+14
    symbol = symbol.replace('&', '_')
    symbol = symbol.replace('-', '_')
    nse_map = {'UNITDSPR': 'MCDOWELL_N',
               'MOTHERSUMI': 'MSUMI'}
    if exchange=='NSE' and symbol in nse_map:
        symbol = nse_map[symbol]
    
    report = []
    
    df = tv.get_hist(
                symbol,
                exchange,
                interval=timeframe,
                n_bars=n_bars,
                extended_session=False,
            )
    if df is None:
        print(f'Error fetching information on {symbol}')
    else:
        try:
            [pos, neg] = detect_divergence(df, indicator='RSI', 
                                           after=after)
            
            if not pos.empty:
                print('Positive divergences')
                print(pos.head())
                for idx, r  in pos.iterrows():
                    entry = get_report_dict(symbol)
                    entry['time'] = idx.strftime('%d/%m/%y %H:%M')
                    entry['rsi'] = r['rsi']
                    entry['divergence'] = 'Positive'
                    entry['region'] = get_rsi_region(r['rsi'])
                    entry['price'] = r['close']
                    entry['cmp'] = df.close[-1]
                    report.append(entry)
                    #report[symbol] = entry
                
            if not neg.empty:
                print('Negative divergences')
                print(neg.head())
                for idx, r in neg.iterrows():
                    entry = get_report_dict(symbol)
                    entry['time'] = idx.strftime('%d/%m/%y %H:%M')
                    entry['rsi'] = r['rsi']
                    entry['divergence'] = 'Negative'
                    entry['region'] = get_rsi_region(r['rsi'])
                    entry['price'] = r['close']
                    entry['cmp'] = df.close[-1]
                    report.append(entry)
                    #report[symbol] = entry
        except:
            print(f'Error in {symbol}')

    try:
        result = rsi_scan(df) 
        if len(result) > 0:
            print(f'{symbol}:\n{result}')
            entry = get_report_dict(symbol)
            entry['time'] = df.index[-1].strftime('%d/%m/%y %H:%M')
            entry['rsi'] = df.rsi[-1]
            entry['region'] = get_rsi_region(df.rsi[-1])
            entry['price'] = df.close[-1]
            entry['cmp'] = df.close[-1]
            report.append(entry)
            #report[symbol] = entry
    except:
        print(f'Exception occured in stock: {symbol}')
        traceback.print_exc()
    
    # if len(report)>0 and report_fd is not None:
    #     for r in report:
    #     #    report_fd.writerow(r)
    #         report_fd.write(json.dumps(report, indent=4))
    return report

def write_report():
    import json
    import csv
    import datetime
    fd = open('report.json', 'r')
    csvfile = open(f'reports/report_{datetime.datetime.today().strftime("%d_%m_%y_%H_%M")}.csv', 'w')
    
    writer = csv.DictWriter(csvfile, fieldnames=get_field_names())
    writer.writeheader()
    
    data = json.load(fd)
    for s in data:
        for r in data[s]:
            #print(r)
            writer.writerow(r)
    
    csvfile.close()
    fd.close()
    
def main(stock_name=None, exchange = 'NSE', timeframe= '15m', date=None, category = '', online=False):
    #List of FnO stocks (where bullish and bearish signals are required)
    writer = None
    jsonfile = None
    report = {}
    if online:
        jsonfile = open('report.json', 'w', newline='')
        #writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        #writer.writeheader()

    timeframe=convert_timeframe_to_quant(timeframe)
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
    after = None
    if date is not None:
        after = date.strftime('%Y-%m-%d %H:%M:00')
        
    if stock_name is None:
        for sname in fno:
            try:
                print(sname)
                stock = Stock.objects.get(sid=sname)
                if not online:
                    do_stuff(stock, prefix='[FnO]', date=date)
                else:
                    r = divergence_scan(exchange=exchange, symbol=sname, timeframe=timeframe, after=after)
                    if len(r)>0:
                        report[sname] = r
                        
            except Stock.DoesNotExist:
                print(f'{sname} name not present')
        if category != 'fno':
            for sname in portfolio:
                try:
                    print(sname)
                    stock = Stock.objects.get(sid=sname)
                    if not online:
                        do_stuff(stock, prefix='[PF]', date=date)
                    else:
                        r = divergence_scan(exchange=exchange, symbol=sname, timeframe=timeframe, after=after)
                        if len(r)>0:
                            report[sname] = r
                except Stock.DoesNotExist:
                    print(f'{sname} name not present')
            for stock in Stock.objects.all():
                #listing = get_stock_listing(stock, duration=30, last_date = datetime.date(2020, 12, 31))
                #print(stock)
                if (stock.sid in fno) or (stock.sid in portfolio):
                    continue
                if stock.sid in margin_stocks:
                    print(sname)
                    if not online:
                        do_stuff(stock, prefix='[MG]', date=date)
                    else:
                        r = divergence_scan(exchange=exchange, symbol=sname, timeframe=timeframe, after=after)
                        if len(r)>0:
                            report[sname] = r
                else:
                    print(sname)
                    if not online:
                        do_stuff(stock, date=date)
                    else:
                        r = divergence_scan(exchange=exchange, symbol=sname, timeframe=timeframe, after=after)
                        if len(r)>0:
                            report[sname] = r
    else:
        prefix = ''
        stock = Stock.objects.get(sid=stock_name)
        if stock in fno:
            prefix='[FnO]'
        elif stock in portfolio:
            prefix='[PF]'
        elif stock in margin_stocks:
            prefix='[MG]'
        print(stock_name)
        if not online:
            do_stuff(stock, prefix=prefix, date=date)
        else:
            r = divergence_scan(exchange=exchange, symbol=stock_name, timeframe=timeframe, after=after)
            if len(r)>0:
                report[stock_name] = r
    
    if jsonfile is not None:
        jsonfile.write(json.dumps(report, indent=4))
        jsonfile.close()
        write_report()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Scan stock info')
    parser.add_argument('-s', '--stock', help="Stock code")
    parser.add_argument('-d', '--date', help="Date")
    parser.add_argument('-e', '--exchange', help="Exchange")
    parser.add_argument('-t', '--timeframe', help="Timeframe")
    parser.add_argument('-f', '--fno', help="Scan FnO stocks only", action='store_true', default=False)
    parser.add_argument('-c', '--category', help="Category:One of 'all', 'fno', 'margin', 'portfolio'")
    parser.add_argument('-o', '--online', action='store_true', default=False, help="Online mode:Fetch from tradingview")
    args = parser.parse_args()
    stock_code = None
    day = None
    exchange = 'NSE'
    timeframe = '1h'
    category = 'all'
    
    if args.stock is not None and len(args.stock)>0:
        print('Scan data for stock {}'.format(args.stock))
        stock_code = args.stock
    if args.date is not None and len(args.date)>0:
        print('Scan data for date: {}'.format(args.date))
        try:
            day = datetime.datetime.strptime(args.date, "%d/%m/%y %H:%M")
        except:
            try:
                day = datetime.datetime.strptime(args.date, "%d/%m/%y")
            except:
                print('Error parsing date')
                day = None
    if args.exchange is not None and len(args.exchange)>0:
        exchange=args.exchange
    if args.timeframe is not None and len(args.timeframe)>0:
        timeframe=args.timeframe
    if args.fno is True:
        category = 'fno'
    if args.category is not None:
        category = args.category

    print('Scan for {}'.format(category))
    main(stock_code, exchange, timeframe = timeframe, category=category, date=day, online=args.online)
