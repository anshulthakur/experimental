from lib import nse
from lib.logging import log, set_loglevel
import json

set_loglevel('debug')
market  = nse.NseIndia(legacy=True)
data = market.getIndexIntradayData(index='NIFTY 50')
print(json.dumps(data, indent=2))
