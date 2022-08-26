import os
import sys
import csv
import urllib
import requests

indices = ["Nifty_50",
            "Nifty_Next_50",
            "Nifty_100",
            "Nifty_200",
            "Nifty_500",
            "Nifty_Midcap_50",
            "NIFTY_Midcap_100",
            "NIFTY_Smallcap_100",
            "Nifty50_Dividend_Points",
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
            "Nifty50_Shariah",
            "Nifty500_Shariah",
            "Nifty_Low_Volatility_50",
            "Nifty_Alpha_50",
            "Nifty_High_Beta_50",
            "Nifty100_Equal_Weight",
            "Nifty100_Liquid_15",
            "Nifty_CPSE",
            "Nifty50_Value_20",
            "Nifty_Midcap_Liquid_15",
            "Nifty_Shariah_25",
            "India_VIX",
            "Nifty_Growth_Sectors_15",
            "Nifty50_TR_1x_Inverse",
            "Nifty50_TR_2x_Leverage",
            "Nifty50_PR_1x_Inverse",
            "Nifty50_PR_2x_Leverage",
            "NIFTY100_Quality_30",
            "Nifty_Mahindra_Group",
            "Nifty_50_Futures_TR_Index",
            "Nifty_Tata_Group",
            "Nifty_Tata_Group_25%_Cap",
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
            "Nifty100_ESG_Sector_Leaders",
            "Nifty200_Alpha_30",
            "Nifty_Midcap150_Momentum_50",
            "NIFTY100_ESG",
            "NIFTY100_Enhanced_ESG",
            "NIFTY50_Equal_Weight",
            "Nifty_50_Futures_Index",
            "Nifty_50_Arbitrage",
            "Nifty_Aditya_Birla_Group",
            "Nifty_8_13_yr_G_Sec",
            "Nifty_4_8_yr_G_Sec_Index",
            "Nifty_11_15_yr_G_Sec_Index",
            "Nifty_15_yr_and_above_G_Sec_Index",
            "Nifty_Composite_G_sec_Index",
            "Nifty_10_yr_Benchmark_G_Sec",
            "Nifty_10_yr_Benchmark_G_Sec_Clean_Price",
            "Nifty_1D_Rate_Index",
            "Nifty50_USD",]

urlmap = {'Nifty_Financial_Services': 'https://www1.nseindia.com/content/indices/niftyfinancelist.csv',
          'Nifty_India_Consumption': 'https://www1.nseindia.com/content/indices/niftyconsumptionlist.csv',
          'Nifty_Dividend_Opportunities_50':'https://www1.nseindia.com/content/indices/niftydivopp50list.csv',
          'Nifty_Infrastructure': 'https://www1.nseindia.com/content/indices/niftyinfralisst.csv',
          'Nifty_Services_Sector': 'https://www1.nseindia.com/content/indices/niftyservicelist.csv',
          'Nifty_Low_Volatility_50': 'https://www1.nseindia.com/content/indices/nifty_Low_Volatility50_Index.csv',
          'Nifty_Alpha_50': 'https://www1.nseindia.com/content/indices/ind_nifty_Alpha_Index.csv',
          "Nifty_High_Beta_50": 'https://www1.nseindia.com/content/indices/nifty_High_Beta50_Index.csv',
          "Nifty100_Equal_Weight": 'https://www1.nseindia.com/content/indices/ind_nifty100list.csv',
          "Nifty100_Liquid_15": 'https://www1.nseindia.com/content/indices/ind_Nifty100_Liquid15.csv',
          "Nifty_CPSE": 'https://www1.nseindia.com/content/indices/ind_niftycpselist.csv',
          "Nifty50_Value_20": 'https://www1.nseindia.com/content/indices/ind_Nifty50_Value20.csv',
          "Nifty_Midcap_Liquid_15": 'https://www1.nseindia.com/content/indices/ind_Nifty_Midcap_Liquid15.csv',
          "Nifty_Growth_Sectors_15": 'https://www1.nseindia.com/content/indices/ind_NiftyGrowth_Sectors15_Index.csv',
          "NIFTY100_Quality_30": 'https://www1.nseindia.com/content/indices/ind_nifty100Quality30list.csv',
          "Nifty_Private_Bank": 'https://www1.nseindia.com/content/indices/ind_nifty_privatebanklist.csv',
          "Nifty_Midcap_Select": 'https://www1.nseindia.com/content/indices/ind_niftymidcapselect_list.csv',
          "NIFTY_SME_EMERGE": 'https://www1.nseindia.com/content/indices/ind_niftysmelist.csv',
          "Nifty_Oil_&_Gas": 'https://www1.nseindia.com/content/indices/ind_niftyoilgaslist.csv',
          "Nifty_Financial_Services_25_50": 'https://www1.nseindia.com/content/indices/ind_niftyfinancialservices25_50list.csv',
          "Nifty_Healthcare_Index": 'https://www1.nseindia.com/content/indices/ind_niftyhealthcarelist.csv',
          "Nifty500_Multicap_50_25_25": 'https://www1.nseindia.com/content/indices/ind_nifty500Multicap502525_list.csv',
          "Nifty_Microcap_250": 'https://www1.nseindia.com/content/indices/ind_niftymicrocap250_list.csv',
          "Nifty_India_Digital": 'https://www1.nseindia.com/content/indices/ind_niftyindiadigital_list.csv',
          "Nifty_Mobility": 'https://www1.nseindia.com/content/indices/ind_niftymobility_list.csv"',
          "Nifty_India_Defence": '"https://www1.nseindia.com/content/indices/ind_niftyindiadefence_list.csv"',
          "Nifty_Financial_Services_Ex_Bank": 'https://www1.nseindia.com/content/indices/ind_niftyfinancialservicesexbank_list.csv',
          "Nifty_Housing": 'https://www1.nseindia.com/content/indices/ind_niftyhousing_list',
        "Nifty_Transportation_&_Logistics": '',
        "Nifty_MidSmall_Financial_Services": '',
        "Nifty_MidSmall_Healthcare": '',
        "Nifty_MidSmall_IT_&_Telecom": '',
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
        "Nifty_50_Arbitrage": '',
        "Nifty_Aditya_Birla_Group": '',
        "Nifty_8_13_yr_G_Sec": '',
        "Nifty_4_8_yr_G_Sec_Index": '',
        "Nifty_11_15_yr_G_Sec_Index": '',
        "Nifty_15_yr_and_above_G_Sec_Index": '',
        "Nifty_Composite_G_sec_Index": '',
        "Nifty_10_yr_Benchmark_G_Sec": '',
        "Nifty_10_yr_Benchmark_G_Sec_Clean_Price": '',
        "Nifty_1D_Rate_Index": '',
        "Nifty50_USD": '',
          }

import os.path
for index in indices:
    ix = index.lower().replace('_','')
    replacements = ['&', '%', '_', '-']
    for r in replacements:
        ix = ix.replace(r,'')
    print(f'"{index}": "https://www1.nseindia.com/content/indices/ind_{ix}list.csv"')
    url = f'https://www1.nseindia.com/content/indices/ind_{ix}list.csv'
    if index in urlmap:
        url = 'nifty'+urlmap[index]

    filepath = './reports/members/'+index+'.csv'
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
    except Exception as e:
        print ('ERR:: Exception occurred while fetching data.')
        print(e)