#Port of https://github.com/hi-imcodeman/stock-nse-india
import datetime
from dateutil import tz
import requests
import time
import urllib.parse
import json
from .logging import log
import pandas as pd
import numpy as np

ApiList = {
    "GLOSSARY" : '/api/cmsContent?url:/glossary',
    "HOLIDAY_TRADING" : '/api/holiday-master?type:trading',
    "HOLIDAY_CLEARING" : '/api/holiday-master?type:clearing',
    "MARKET_STATUS" : '/api/marketStatus',
    "MARKET_TURNOVER" : '/api/market-turnover',
    "ALL_INDICES" : '/api/allIndices',
    "INDEX_NAMES" : '/api/index-names',
    "CIRCULARS" : '/api/circulars',
    "LATEST_CIRCULARS" : '/api/latest-circular',
    "EQUITY_MASTER" : '/api/equity-master',
    "MARKET_DATA_PRE_OPEN" : '/api/market-data-pre-open?key:ALL',
    "MERGED_DAILY_REPORTS_CAPITAL" : '/api/merged-daily-reports?key:favCapital',
    "MERGED_DAILY_REPORTS_DERIVATIVES" : '/api/merged-daily-reports?key:favDerivatives',
    "MERGED_DAILY_REPORTS_DEBT" : '/api/merged-daily-reports?key:favDebt'
}

def getDateRangeChunks(startDate, endDate, chunkInDays):
    pass
    '''
    const range = moment.range(startDate, endDate)
    const chunks = Array.from(range.by('days', { step: chunkInDays }))
    const 
    for (let i = 0; i < chunks.length; i++) {
        dateRanges.push({
            start: i > 0 ? chunks[i].add(1, 'day').format('DD-MM-YYYY') : chunks[i].format('DD-MM-YYYY'),
            end: chunks[i + 1] ? chunks[i + 1].format('DD-MM-YYYY') : range.end.format('DD-MM-YYYY')
        })
    }
    '''
    dateRanges = []
    return dateRanges

class NseIndia(object):
    def __init__(self, timeout=10, legacy = False):
        self.baseUrl = 'https://www.nseindia.com'
        self.legacyBaseUrl = 'https://www1.nseindia.com'
        self.cookieMaxAge = 300 # should be in seconds
        self.cookies = []
        self.cookieExpiry = datetime.datetime.now() + datetime.timedelta(seconds=self.cookieMaxAge)
        user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36 Edg/107.0.1418.35'
        self.baseHeaders = {
            "accept-encoding": "gzip, deflate, br",
            "accept":
            """text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9""",
            "accept-language": "en-US,en;q=0.9",
            "host": "www.nseindia.com",
            "referer": "https://www.nseindia.com",
            'Connection': 'keep-alive',
            "user-agent": user_agent
        }
        
        self.timeout = timeout
        self.legacy = legacy
        self.session = requests.Session()
        self.session.headers.update(self.baseHeaders)
    
    def getNseCookies(self):
        #log('getNseCookies', 'debug')
        if len(self.cookies)==0 or self.cookieExpiry <= datetime.datetime.now():
            response = self.session.get(self.legacyBaseUrl if self.legacy else self.baseUrl, timeout=self.timeout)
            setCookies = response.headers['set-cookie']
            cookies = []
            for cookie in setCookies:
                requiredCookies = ['nsit', 'nseappid', 'ak_bmsc', 'AKA_A2']
                cookieKeyValue = cookie.split(';')[0]
                cookieEntry = cookieKeyValue.split('=')
                if (cookieEntry[0] in requiredCookies):
                    cookies.append(cookieKeyValue)
            log(f"Cookies: {cookies}", "debug")
            self.cookies = '; '.join(cookies)
            self.cookieExpiry = datetime.datetime.now() + datetime.timedelta(seconds=self.cookieMaxAge)

        return self.cookies

    '''
     @param url NSE API's URL
     @returns JSON data from NSE India
    '''
    def getData(self, url):
        #log('getData', 'debug')
        retries = 0
        hasError = True
        while hasError:
            hasError = False
            try:
                response = self.session.get(url, 
                                        headers= self.baseHeaders, #.update(map('Cookie', self.getNseCookies())),
                                        timeout=self.timeout
                                        )
                return response.text
            except:
                hasError = True
                retries +=1
                if (retries >= 10):
                    raise
    
    '''
     @param apiEndpoint 
     @param isLegacy 
     @returns 
     '''
    def getDataByEndpoint(self, apiEndpoint, isLegacy = False):
        #log('getDataByEndpoint', 'debug')
        if not isLegacy:
            return self.getData(url = self.baseUrl + apiEndpoint)
        else:
            return self.getData(url = self.legacyBaseUrl + apiEndpoint)
    
    '''
     @returns List of NSE equity symbols
     '''
    def getAllStockSymbols(self):
        log('getAllStockSymbols', 'debug')
        data = self.getDataByEndpoint(ApiList.MARKET_DATA_PRE_OPEN)
        symbols = []
        for obj in data:
            symbols.append(obj['metadata']['symbol'])
        return symbols

    '''
     @param symbol 
     @returns 
    '''
    def getEquityDetails(self, symbol):
        log('getEquityDetails', 'debug')
        return self.getDataByEndpoint(f"/api/quote-equity?symbol={urllib.parse.urlencode(symbol)}")

    '''
     @param symbol 
     @returns 
    '''
    def getEquityTradeInfo(self, symbol):
        log('getEquityTradeInfo', 'debug')
        return self.getDataByEndpoint(f"/api/quote-equity?symbol={urllib.parse.urlencode(symbol)}&section=trade_info")
    
    '''
     @param symbol 
     @returns 
    '''
    def getEquityCorporateInfo(self, symbol):
        log('getEquityCorporateInfo', 'debug')
        return self.getDataByEndpoint(f"/api/quote-equity?symbol={urllib.parse.urlencode(symbol)}&section=corp_info")
    
    '''
     @param symbol 
     @param isPreOpenData 
     @returns 
    '''
    def getEquityIntradayData(self, symbol, isPreOpenData = False):
        log('getEquityIntradayData', 'debug')
        details = self.getEquityDetails(symbol)
        identifier = details.info.identifier
        url = f"/api/chart-databyindex?index={identifier}"
        if (isPreOpenData):
            url += '&preopen=true'
        return self.getDataByEndpoint(url)

    '''
     @param symbol 
     @param range 
     @returns 
    '''
    def getEquityHistoricalData(self, symbol, range=None): 
        log('getEquityHistoricalData', 'debug')
        if (range is None):
            data =  self.getEquityDetails(symbol)
            range = { 'start': datetime.datetime.strptime(data['metadata']['listingDate'], '%D/%M/%Y'), 
                     'end': datetime.datetime.today() }

        dateRanges = getDateRangeChunks(range['start'], range['end'], 66)
        '''
        promises = dateRanges.map(async (dateRange) => {
            const url = `/api/historical/cm/equity?symbol=${encodeURIComponent(symbol)}` +
                `&series=[%22EQ%22]&from=${dateRange.start}&to=${dateRange.end}`
            return this.getDataByEndpoint(url)
        })
        '''
        #Evaluate all
        data = []
        #return Promise.all(promises)
        return data

    '''
     @param symbol 
     @returns 
    '''
    def getEquitySeries(self, symbol):
        log('getEquitySeries', 'debug')
        return self.getDataByEndpoint(f"/api/historical/cm/equity/series?symbol={urllib.parse.urlencode(symbol)}")

    '''
     @param index 
     @returns 
    '''
    def getEquityStockIndices(self, index):
        log('getEquityStockIndices', 'debug')
        return self.getDataByEndpoint(f"/api/equity-stockIndices?index={urllib.parse.urlencode(index)}")

    '''
     @param index 
     @param isPreOpenData 
     @returns 
    '''
    def getIndexIntradayData(self, index, isPreOpenData = False, resample=None):
        #log('getIndexIntradayData', 'debug') 
        endpoint = f"/api/chart-databyindex?index={index}&indices=true"
        if (isPreOpenData):
            endpoint += '&preopen=true'
        data = json.loads(self.getDataByEndpoint(endpoint))
        india_tz= tz.gettz('UTC')
        df = pd.DataFrame.from_records(data['grapthData'], columns=['datetime', 'close'])
        df['datetime'] = pd.to_datetime(df['datetime'], unit='ms')
        df.drop_duplicates(['datetime'], inplace=True)
        df.set_index('datetime', inplace=True)
        df.sort_index(inplace=True)

        logic = {'open'  : 'first',
                 'high'  : 'max',
                 'low'   : 'min',
                 'close' : 'last'}
        if resample is not None and isinstance(resample, str):
            df = df.resample(resample).ohlc()
            df = df.droplevel(level=0, axis=1)
            #df = df.resample(resample).apply(logic)
        return df

    '''
     @param index 
     @param range 
     @returns 
    '''
    def getIndexHistoricalData(index, range):
        log('getIndexHistoricalData', 'debug') 
        dateRanges = getDateRangeChunks(range['start'], range['end'], 360)
        '''
        const promises = dateRanges.map(async (dateRange) => {
            const endpoint = '/products/dynaContent/equities/indices/historicalindices.jsp' +
                `?indexType=${encodeURIComponent(index)}&fromDate=${dateRange.start}&toDate=${dateRange.end}`
            const html: string = await this.getDataByEndpoint(endpoint, true)
            const $ = cheerio.load(html)
            const historical: any[] = []
            const historicalRecords = $('#csvContentDiv').text().split(':')
            historicalRecords.forEach((record: string, i: number) => {
                if (record && i > 0) {
                    const [date, open, high, low, close, volume, turnover] = record.split(',').map(item => {
                        item = item.replace(/[",\s]/g, '')
                        return item
                    })
                    historical.push({
                        date: new Date(`${date} 17:30:00 GMT+0530`),
                        open: Number(open),
                        high: Number(high),
                        low: Number(low),
                        close: Number(close),
                        volume: Number(volume),
                        turnoverInCrore: Number(turnover)
                    })
                }

            })
            return historical
        })
        const historicalDataArray = await Promise.all(promises)
        '''
        historicalDataArray = []
        historicalData = []

        for h in historicalDataArray:
            historicalData += historicalData.concat(h)
        return {
            'indexSymbol': index,
            'fromDate': range['start'],
            'toDate': range['end'],
            'data': historicalData
        }
