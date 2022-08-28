import os
import sys
import csv
import urllib
import requests


members_dir = './reports/members/'
indices = ["Nifty_50",
           "Nifty_Next_50",
           "Nifty_100",
           "Nifty_200",
           "Nifty_500",
           "Nifty_Midcap_50",
           "NIFTY_Midcap_100",
           "NIFTY_Smallcap_100",
           "Nifty_Auto",
           "Nifty_Bank",
           "Nifty_Energy",
           "Nifty_Financial_Services",
           "Nifty_FMCG",
           "Nifty_IT",
           "Nifty_Media",
           "Nifty_Metal",
           "Nifty_MNC",
           "Nifty_Pharma",
           "Nifty_PSU_Bank",
           "Nifty_Realty",
           "Nifty_India_Consumption",
           "Nifty_Commodities",
           "Nifty_Dividend_Opportunities_50",
           "Nifty_Infrastructure",
           "Nifty_PSE",
           "Nifty_Services_Sector",
           "Nifty_Low_Volatility_50",
           "Nifty_Alpha_50",
           "Nifty_High_Beta_50",
           "Nifty100_Equal_Weight",
           "Nifty100_Liquid_15",
           "Nifty_CPSE",
           "Nifty50_Value_20",
           "Nifty_Midcap_Liquid_15",
           "Nifty_Growth_Sectors_15",
           "NIFTY100_Quality_30",
           "Nifty_Private_Bank",
           "Nifty_Smallcap_250",
           "Nifty_Smallcap_50",
           "Nifty_MidSmallcap_400",
           "Nifty_Midcap_150",
           "Nifty_Midcap_Select",
           "NIFTY_LargeMidcap_250",
           "NIFTY_SME_EMERGE",
           "Nifty_Oil_&_Gas",
           "Nifty_Financial_Services_25_50",
           "Nifty_Healthcare_Index",
           "Nifty500_Multicap_50_25_25",
           "Nifty_Microcap_250",
           "Nifty_Total_Market",
           "Nifty_India_Digital",
           "Nifty_Mobility",
           "Nifty_India_Defence",
           "Nifty_Financial_Services_Ex_Bank",
           "Nifty_Housing",
           "Nifty_Transportation_&_Logistics",
           "Nifty_MidSmall_Financial_Services",
           "Nifty_MidSmall_Healthcare",
           "Nifty_MidSmall_IT_&_Telecom",
           "Nifty_Consumer_Durables",
           "Nifty_Non_Cyclical_Consumer",
           "Nifty200_Momentum_30",
           "NIFTY100_Alpha_30",
           "NIFTY500_Value_50",
           "Nifty100_Low_Volatility_30",
           "NIFTY_Alpha_Low_Volatility_30",
           "NIFTY_Quality_Low_Volatility_30",
           "NIFTY_Alpha_Quality_Low_Volatility_30",
           "NIFTY_Alpha_Quality_Value_Low_Volatility_30",
           "NIFTY200_Quality_30",
           "NIFTY_Midcap150_Quality_50",
           "Nifty_India_Manufacturing",
           "Nifty200_Alpha_30",
           "Nifty_Midcap150_Momentum_50",
           "NIFTY50_Equal_Weight",
           ]

urlmap = {
         "Nifty_India_Consumption": "https://www1.nseindia.com/content/indices/ind_niftyconsumptionlist.csv",
         "Nifty_Dividend_Opportunities_50": "https://www1.nseindia.com/content/indices/ind_niftydivopp50list.csv",
         "Nifty_Infrastructure": "https://www1.nseindia.com/content/indices/ind_niftyinfralist.csv",
         "Nifty_Services_Sector": "https://www1.nseindia.com/content/indices/ind_niftyservicelist.csv",
         "Nifty_Low_Volatility_50": "https://www1.nseindia.com/content/indices/nifty_Low_Volatility50_Index.csv",
         "Nifty_Alpha_50": "https://www1.nseindia.com/content/indices/ind_nifty_Alpha_Index.csv",
         "Nifty_High_Beta_50": "https://www1.nseindia.com/content/indices/nifty_High_Beta50_Index.csv",
         "Nifty100_Equal_Weight": "https://www1.nseindia.com/content/indices/ind_nifty100list.csv",
         "Nifty100_Liquid_15": "https://www1.nseindia.com/content/indices/ind_Nifty100_Liquid15.csv",
         "Nifty_CPSE": "https://www1.nseindia.com/content/indices/ind_niftycpselist.csv",
         "Nifty50_Value_20": "https://www1.nseindia.com/content/indices/ind_Nifty50_Value20.csv",
         "Nifty_Midcap_Liquid_15": "https://www1.nseindia.com/content/indices/ind_Nifty_Midcap_Liquid15.csv",
         "Nifty_Growth_Sectors_15": "https://www1.nseindia.com/content/indices/ind_NiftyGrowth_Sectors15_Index.csv",
         "NIFTY100_Quality_30": "https://www1.nseindia.com/content/indices/ind_nifty100Quality30list.csv",
         "Nifty_Private_Bank": "https://www1.nseindia.com/content/indices/ind_nifty_privatebanklist.csv",
         "Nifty_Midcap_Select": "https://www1.nseindia.com/content/indices/ind_niftymidcapselect_list.csv",
         "NIFTY_SME_EMERGE": "https://www1.nseindia.com/content/indices/ind_niftysmelist.csv",
         "Nifty_Oil_&_Gas": "https://www1.nseindia.com/content/indices/ind_niftyoilgaslist.csv",
         "Nifty_Financial_Services_25_50": "https://www1.nseindia.com/content/indices/ind_niftyfinancialservices25_50list.csv",
         "Nifty_Healthcare_Index": "https://www1.nseindia.com/content/indices/ind_niftyhealthcarelist.csv",
         "Nifty500_Multicap_50_25_25": "https://www1.nseindia.com/content/indices/ind_nifty500Multicap502525_list.csv",
         "Nifty_Microcap_250": "https://www1.nseindia.com/content/indices/ind_niftymicrocap250_list.csv",
         "Nifty_India_Digital": "https://www1.nseindia.com/content/indices/ind_niftyindiadigital_list.csv",
         "Nifty_Mobility": "https://www1.nseindia.com/content/indices/ind_niftymobility_list.csv",
         "Nifty_India_Defence": "\"https://www1.nseindia.com/content/indices/ind_niftyindiadefence_list.csv\"",
         "Nifty_Financial_Services_Ex_Bank": "https://www1.nseindia.com/content/indices/ind_niftyfinancialservicesexbank_list.csv",
         "Nifty_Housing": "https://www1.nseindia.com/content/indices/ind_niftyhousing_list.csv",
         "NIFTY500_Value_50": "https://www1.nseindia.com/content/indices/ind_nifty500Value50_list.csv",
         "Nifty200_Momentum_30": "https://www1.nseindia.com/content/indices/ind_nifty200Momentum30_list.csv",
         "NIFTY100_Alpha_30": "https://www1.nseindia.com/content/indices/ind_nifty100Alpha30list.csv",
         "Nifty100_Low_Volatility_30": "https://www1.nseindia.com/content/indices/ind_Nifty100LowVolatility30list.csv",
         "NIFTY200_Quality_30": "https://www1.nseindia.com/content/indices/ind_nifty200Quality30_list.csv"
        }


'''
      'Nifty_Financial_Services': 'https://www.niftyindices.com/IndexConstituent/ind_niftyfinancelist.csv',
      "Nifty_Transportation_&_Logistics": 'https://www.niftyindices.com/IndexConstituent/ind_niftytransportandlogistics_list.csv',
      "Nifty_MidSmall_Financial_Services": 'https://www.niftyindices.com/IndexConstituent/ind_niftymidsmallfinancialservice_list.csv',
      "Nifty_MidSmall_IT_&_Telecom": 'https://www.niftyindices.com/IndexConstituent/ind_niftymidsmallitAndtelecom_list.csv',

        "Nifty_MidSmall_Healthcare": '',
        "Nifty_Consumer_Durables": '',
        "Nifty_Non_Cyclical_Consumer": '',
        "Nifty200_Momentum_30": '',
        "NIFTY100_Alpha_30": '',
        "NIFTY500_Value_50": '',
        "Nifty100_Low_Volatility_30": '',
        "NIFTY_Alpha_Low_Volatility_30": '',
        "NIFTY_Quality_Low_Volatility_30": '',
        "NIFTY_Alpha_Quality_Low_Volatility_30": '',
        "NIFTY_Alpha_Quality_Value_Low_Volatility_30": '',
        "NIFTY200_Quality_30": '',
        "NIFTY_Midcap150_Quality_50": '',
        "Nifty_India_Manufacturing": '',
        "Nifty100_ESG_Sector_Leaders": '',
        "Nifty200_Alpha_30": '',
        "Nifty_Midcap150_Momentum_50": '',
        "NIFTY100_ESG": '',
        "NIFTY100_Enhanced_ESG": '',
        "NIFTY50_Equal_Weight": '',
        "Nifty_50_Futures_Index": '',
'''

import os.path
for index in indices:
    ix = index.lower().replace('_','')
    replacements = ['&', '%', '_', '-', '(', ')']
    for r in replacements:
        ix = ix.replace(r,'')
    print(f'"{index}": "https://www1.nseindia.com/content/indices/ind_{ix}list.csv"')
    url = f'https://www1.nseindia.com/content/indices/ind_{ix}list.csv'
    if index in urlmap:
        url = urlmap[index]
    print(url)
    
    filepath = members_dir+index+'.csv'
    if os.path.isfile(filepath):
        print(f'File: {filepath} exists. Skip download')
        continue
    headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:57.0) Gecko/20100101 Firefox/57.0', 
            'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',
            }
    
    try:
        session = requests.Session()
        response = session.get(url, headers=headers)
        response.raise_for_status()
        open(filepath, 'wb').write(response.content)
        urlmap[index]= url
    except:
        try:
            print('retry 1')
            url = f'https://www1.nseindia.com/content/indices/ind_{ix}_list.csv'
            session = requests.Session()
            response = session.get(url, headers=headers)
            response.raise_for_status()
            open(filepath, 'wb').write(response.content)
            urlmap[index]= url
        except Exception as e:
                    print ('ERR:: Exception occurred while fetching data.')
                    print(e)
        #The niftyindices doesn't allow downloads like this somehow
        '''
        except:
            try:
                print('retry 2')
                url = f'https://www.niftyindices.com/IndexConstituent/ind_{ix}_list.csv'
                session = requests.Session()
                response = session.get(url, headers=headers)
                response.raise_for_status()
                open(filepath, 'wb').write(response.content)
                urlmap[index]= url
            except:
                try:
                    print('retry 3')
                    url = f'https://www.niftyindices.com/IndexConstituent/ind_{ix}list.csv'
                    session = requests.Session()
                    response = session.get(url, headers=headers)
                    response.raise_for_status()
                    open(filepath, 'wb').write(response.content)
                    urlmap[index]= url
                except Exception as e:
                    print ('ERR:: Exception occurred while fetching data.')
                    print(e)
        '''
import json
print(json.dumps(urlmap, indent = 1))

'''
Now that we have the members map, have a membership report for reverse lookup 
for easy readability.

What is expected is that the consumer of this information should be able to get
a snapshot of the company profile in terms of its market participation

For example:
ACC:
Company: ACC Ltd.
Industry: Construction Materials
Indices: A,B,C
'''

def get_stock_info(symbol):
    return {'symbol': symbol,
            'name': None,
            'industry': [],
            'indices': []}

stocks = {}

from os import listdir
from os.path import isfile, join
files = [f for f in listdir(members_dir) if isfile(join(members_dir, f))]

for f in files:
    fname = f.split('.')[-2].replace('_', ' ')
    print(fname)
    with open(join(members_dir, f), 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            index = row['Symbol'].strip()
            if index not in stocks:
                stocks[index]= get_stock_info(index)
                if 'Company Name' in row:
                    stocks[index]['name'] = row['Company Name'].strip()
                else:
                    stocks[index]['name'] = row['Company'].strip()
                stocks[index]['industry'].append(row['Industry'].strip())
                stocks[index]['indices'].append(fname)
            else:
                if 'Company Name' in row:
                    if stocks[index]['name'] != row['Company Name'].strip():
                        print(f'Inconsistent names for symbol {index}')
                        stocks[index]['name'] = row['Company Name'].strip()
                else:
                    if stocks[index]['name'] != row['Company'].strip():
                        print(f'Inconsistent names for symbol {index}')
                        stocks[index]['name'] = row['Company'].strip()
                if fname not in stocks[index]['indices']:
                    stocks[index]['indices'].append(fname)
                if row['Industry'].strip() not in stocks[index]['industry']:
                    stocks[index]['industry'].append(row['Industry'].strip())

print(json.dumps(stocks, indent = 1))