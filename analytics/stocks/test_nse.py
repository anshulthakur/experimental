from lib import nse
from lib.logging import log, set_loglevel
import json

set_loglevel('debug')
market  = nse.NseIndia(legacy=False)
#data = market.getIndexIntradayData(index='NIFTY 50', resample='5min')
data = market.getEquityStockIndices(index='NIFTY TOTAL MARKET')
print(data.head(20))
