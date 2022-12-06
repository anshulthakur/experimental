'''
On a given time scale, determine whether the security is in uptrend, or downtrend, or no-trend
'''
import os
import sys
import settings
import numpy as np
import pandas as pd

import datetime
from stocks.models import Listing, Stock, Market

from lib.retrieval import get_stock_listing
from lib.tradingview import convert_timeframe_to_quant, get_tvfeed_instance
from lib.pivots import getHigherHighs, getLowerHighs, getLowerLows, getHigherLows
from lib.logging import set_loglevel, log
import json

#Plotting
from matplotlib.lines import Line2D # For legend
import matplotlib.pyplot as plt
#from mplfinance.original_flavor import candlestick_ohlc
#import matplotlib.dates as mpl_dates

#from matplotlib.dates import date2num


report_dir = './reports/scanners/'
report = {}

def save_plot(stock, df, hh, hl, lh, ll, order=1):
    log('Saving plot', logtype='debug')
    dates = df.index
    price = df['close'].values

    hh_idx = np.array([min(i[1] , len(df)-1) for i in hh])
    lh_idx = np.array([min(i[1] , len(df)-1) for i in lh])
    ll_idx = np.array([min(i[1] , len(df)-1) for i in ll])
    hl_idx = np.array([min(i[1] , len(df)-1) for i in hl])
    
    colors = plt.rcParams['axes.prop_cycle'].by_key()['color']
    plt.figure(figsize=(12, 8))
    plt.plot(df['close'])
    legend_elements = [
                    Line2D([0], [0], color=colors[0], label='Close')
                    ]
    if len(hh_idx)>0:
        plt.scatter(dates[hh_idx], price[hh_idx]+5, marker='v', c=colors[1])
        _ = [plt.plot(dates[i], price[i], c=colors[1]) for i in hh]
        legend_elements.append(Line2D([0], [0], color=colors[1], label='Higher Highs'))
        legend_elements.append(Line2D([0], [0], color='w',  marker='v',
                                        markersize=10,
                                        markerfacecolor=colors[1],
                                        label='Higher Highs Confirmation'))
    if len(hl_idx)>0:
        plt.scatter(dates[hl_idx], price[hl_idx]-5, marker='^', c=colors[2])
        _ = [plt.plot(dates[i], price[i], c=colors[2]) for i in hl]
        legend_elements.append( Line2D([0], [0], color=colors[2], label='Higher Lows'))
        legend_elements.append(Line2D([0], [0], color='w',  marker='^',
                                    markersize=10,
                                    markerfacecolor=colors[2],
                                    label='Higher Lows Confirmation'))
    if len(lh_idx)>0:
        plt.scatter(dates[lh_idx], price[lh_idx]+5, marker='v', c=colors[3])
        _ = [plt.plot(dates[i], price[i], c=colors[3]) for i in lh]
        legend_elements.append(Line2D([0], [0], color=colors[3], label='Lower Highs'))
        legend_elements.append(Line2D([0], [0], color='w',  marker='v',
                                        markersize=10,
                                        markerfacecolor=colors[3],
                                        label='Lower Highs Confirmation'))
    if len(ll_idx)>0:
        plt.scatter(dates[ll_idx], price[ll_idx]-5, marker='^', c=colors[4])
        _ = [plt.plot(dates[i], price[i], c=colors[4]) for i in ll]
        legend_elements.append(Line2D([0], [0], color=colors[4], label='Lower Lows'))
        legend_elements.append(Line2D([0], [0], color='w',  marker='^',
                                        markersize=10,
                                        markerfacecolor=colors[4],
                                        label='Lower Lows Confirmation'))

    plt.xlabel('Date')
    plt.ylabel('Price (Rs)')
    plt.title(f'Potential Divergence Points for  Closing Price')
    plt.legend(handles=legend_elements, bbox_to_anchor=(1, 0.65))
    #plt.show()
    plt.savefig(f"{report_dir}{stock}.jpg", bbox_inches="tight",
            pad_inches=0.3, transparent=False)
    plt.close()

def get_report_handle():
    global report
    return report

def get_dataframe(stock, market, timeframe, date, online=False):
    duration = 60
    if 'w' in timeframe.lower():
        duration = duration * 5 
    if 'm' in timeframe.lower():
        duration = duration * 25

    if online or timeframe.strip().lower()[-1] not in ['d', 'w', 'm']:
        #Either we're explicitly told to fetch data from TV, or timeframe is shorter than a day
        username = 'AnshulBot'
        password = '@nshulthakur123'

        tv = get_tvfeed_instance(username, password)
        interval=convert_timeframe_to_quant(timeframe)

        symbol = stock.symbol.strip().replace('&', '_')
        symbol = symbol.replace('-', '_')
        nse_map = {'UNITDSPR': 'MCDOWELL_N',
                    'MOTHERSUMI': 'MSUMI'}
        if symbol in nse_map:
            symbol = nse_map[symbol]
        s_df = tv.get_hist(
                            symbol,
                            market.name,
                            interval=interval,
                            n_bars=duration,
                            extended_session=False,
                        )
        if len(s_df)==0:
            log('Skip {}'.format(symbol), logtype='warning')
            pass
    else:
        s_df = get_stock_listing(stock, duration=duration, last_date = date, 
                                    resample=True if 'w' in timeframe.lower() else False, 
                                    monthly=True if 'm' in timeframe.lower() else False)
        s_df = s_df.drop(columns = ['volume', 'delivery', 'trades'])
        if len(s_df)==0:
            log('Skip {}'.format(stock.symbol), logtype='warning')
    return s_df

def get_trend(stock, market, timeframe, date, saveplot=False, online=False):
    report = get_report_handle()
    df = get_dataframe(stock, market, timeframe, date, online)

    hh_val = 0
    hl_val = 0
    lh_val = 0
    ll_val = 0
    order = 1

    hh_id = -1
    hl_id = -1
    lh_id = -1
    ll_id = -1

    hh = getHigherHighs(df.close.values, order=order)
    hh_idx = np.array([min(i[1] , len(df)-1) for i in hh])
    if hh_idx is not None and len(hh_idx)>1:
        hh_id = hh_idx[-1]-order
        hh_val = df.close[hh_id]

    hl = getHigherLows(df.close.values, order=order)
    hl_idx = np.array([min(i[1] , len(df)-1) for i in hl])
    if hl_idx is not None and len(hl_idx)>1:
        hl_id = hl_idx[-1]-order
        hl_val = df.close[hl_id]

    ll = getLowerLows(df.close.values, order=order)
    ll_idx = np.array([min(i[1] , len(df)-1) for i in ll])
    if ll_idx is not None and len(ll_idx)>1:
        ll_id = ll_idx[-1]-order
        ll_val = df.close[ll_id]

    lh = getLowerHighs(df.close.values, order=order)
    lh_idx = np.array([min(i[1] , len(df)-1) for i in lh])
    if lh_idx is not None and len(lh_idx)>1:
        lh_id = lh_idx[-1]-order
        lh_val = df.close[lh_id]

    log(f'\n{stock.symbol}: LL: {ll_id} LH: {lh_id} HH: {hh_id} HL: {hl_id}', logtype='debug')
    log(df.head(10), logtype='debug')
    if hh_id>ll_id and hl_id>ll_id:
        if df.close[-1] >= hh_val:
            report[stock.symbol] = 'Uptrend'
            log(f'{stock.symbol}: Uptrend', logtype='debug')
        elif hh_id>hl_id and df.close[-1] >= hl_val:
            report[stock.symbol] = 'Uptrend'
            log(f'{stock.symbol}: Uptrend', logtype='debug')
        else:
            if hl_id > hh_id and df.close[-1] >= hl_val:
                report[stock.symbol] = 'Uptrend (pending confirmation)'
                log(f'{stock.symbol}: Uptrend (pending confirmation)', logtype='debug')
    if lh_id > hh_id and lh_id > hl_id:
        if df.close[-1] < ll_val:
            report[stock.symbol] = 'Downtrend'
            log(f'{stock.symbol}: Downtrend', logtype='debug')
            log(f'{df.close[-1]} < {ll_val}', logtype='debug')
        elif ll_id > lh_id and df.close[-1] >= ll_val and df.close[-1] < lh_val:
            report[stock.symbol] = 'Downtrend'
            log(f'{stock.symbol}: Downtrend', logtype='debug')
            log(f'{df.close[-1]} < {ll_val}', logtype='debug')
        else:
            if lh_id > ll_id and df.close[-1] >= ll_val:
                report[stock.symbol] = 'Downtrend (pending confirmation)'
                log(f'{stock.symbol}: Downtrend (pending confirmation)', logtype='debug')
                log(f'{df.close[-1]} >= {ll_val}', logtype='debug')

    if saveplot:
        if stock.symbol in report:
            save_plot(stock.symbol, df, hh, hl, lh, ll, order)

def main(stock_name=None, exchange = None, timeframe= '1d', date=None, online=False):
    market = None
    if exchange is not None:
        try:
            market = Market.objects.get(name=exchange)
        except Market.DoesNotExist:
            log(f"No object exists for {exchange}", logtype='error')
            return
        if stock_name is None:
            for stock in Stock.objects.filter(market=market):
                try:
                    get_trend(stock, market, timeframe, date, online)
                except:
                    pass
            pass
        else:
            try:
                stock = Stock.objects.get(symbol=stock_name, market=market)
                get_trend(stock, market, timeframe, date, saveplot=True, online=online )
            except Stock.DoesNotExist:
                log(f"Stock with symbol {stock_name} not found in {exchange}", logtype='error')
                return
    else:
        if stock_name is None:
            for stock in Stock.objects.all():
                try:
                    get_trend(stock, market, timeframe, date, online)
                except:
                    pass
        else:
            log('Also specify listing exchange for security', logtype='error')
            return

if __name__ == "__main__":
    set_loglevel('info')
    import argparse
    parser = argparse.ArgumentParser(description='Scan stock securities for trend')
    parser.add_argument('-s', '--stock', help="Stock code")
    parser.add_argument('-e', '--exchange', help="Exchange")
    parser.add_argument('-t', '--timeframe', help="Timeframe")
    parser.add_argument('-d', '--date', help="Date")
    parser.add_argument('-l', '--list', help="List of stocks to scan (MARKET:SYMBOL)")
    parser.add_argument('-o', '--online', action='store_true', default=False, help="Online mode:Fetch from tradingview")
    args = parser.parse_args()
    stock_code = None
    day =  datetime.datetime.now()
    exchange = 'NSE'
    timeframe = '1d'
    category = 'all'
    
    if args.stock is not None and len(args.stock)>0:
        log('Scan data for stock {}'.format(args.stock), logtype='info')
        stock_code = args.stock
    if args.date is not None and len(args.date)>0:
        log('Scan data for date: {}'.format(args.date), logtype='info')
        try:
            day = datetime.datetime.strptime(args.date, "%d/%m/%y %H:%M")
        except:
            try:
                day = datetime.datetime.strptime(args.date, "%d/%m/%y")
            except:
                log('Error parsing date', logtype='error')
                day = None
    if args.exchange is not None and len(args.exchange)>0:
        exchange=args.exchange
    if args.timeframe is not None and len(args.timeframe)>0:
        timeframe=args.timeframe
    
    if args.list is None:
        main(stock_code, exchange, timeframe = timeframe, date=day, online=args.online)
    else:
        stock_list = []
        with open(args.list, 'r') as fd:
            for line in fd:
                stock_list.append(line.strip().upper())
        for stock in stock_list:
            s = stock.split(':')[1]
            m = stock.split(':')[0]
            main(s, m, timeframe = timeframe, date=day, online=args.online)
    
    report = get_report_handle()
    log(json.dumps(report, indent=2, sort_keys=True), logtype='info')