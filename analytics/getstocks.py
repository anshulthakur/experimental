#!/home/anshul/web/analytics/env/bin/python

from nsetools import Nse
import sys
import os
import re
import csv
import datetime

def get_nse_quote(nse, stock):
  return nse.get_quote(stock)
  
#Open NSE Stock list
BASE_PATH = "/home/anshul/web/analytics/analytics/stocks/"
NSE_EQUITY_LIST = "nse_list.csv"
BSE_EQUITY_LIST = "ListOfScripts.csv"

nse_list = open(BASE_PATH+NSE_EQUITY_LIST, "r")

#For each equity, there must be a folder in 'data'
contents = csv.reader(nse_list, delimiter=',')

time = datetime.datetime.now()
nse = Nse()

first_line = True
for row in contents:
  #For each market applicable, there must be 2 files. 
  #One contains Daily data. Other contains hourly data. Append to those files.
  if first_line is True:
    first_line = False
    continue
  
  if os.path.exists(BASE_PATH+'data/'+row[0].lower()):
    if time.hour < 10:
      #Markets have not opened. Record last day's end data
      nse_daily = open(BASE_PATH+'data/'+row[0].lower()+'nse_daily.csv', 'a')
      val = get_nse_quote(nse, row[0])
      writer = csv.writer(nse_daily, delimiter=',', quoting=csv.QUOTE_MINIMAL)
      #Day, adhocMargin, applicableMargin, averagePrice, bcEndDate, bcStartDate, change, closePrice, cm_adj_high_dt, cm_adj_low_dt, cm_ffm, companyName, css_status_desc, dayHigh, dayLow, deliveryQuantity, deliveryToTradedQuantity, exDate, extremeLossMargin, faceValue, high52, indexVar, isExDateFlag, isinCode, lastPrice, low52, marketType, ndEndDate, ndStartDate, open, pChange, previousClose, priceBand, pricebandlower, pricebandupper, purpose, quantityTraded, recordDate, secDate, securityVar, series, surv_indicator, symbol, totalTradedValue, totalTradedVolume, varMargin
          
