import os
import sys
import init
import matplotlib
import numpy as np
import matplotlib.pyplot as plt

from datetime import datetime

from stocks.models import Listing, Stock

def get_data(stock, start_date=None, end_date=None, field='closing'):

  listing = Listing.objects.filter(stock=stock)
  if start_date is not None:
    listing = listing.filter(date__gte=start_date)
  if end_date is not None:
    listing = listing.filter(date__lte=end_date)

  listing = listing.order_by('date')

  return list(map(lambda d:getattr(d,field), listing))


sid = 500820
stock = Stock.objects.get(security=sid)

start_date = datetime.strptime('01/01/2020', '%d/%m/%Y')
end_date = datetime.strptime('31/01/2020', '%d/%m/%Y')
closing = get_data(stock=stock, start_date=start_date, end_date=end_date, field='closing')

#dates = list(map(lambda d: d.date.date(), listing))
#closing = list(map(lambda d:d.closing, listing))

import matplotlib.dates as mdates

#plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d/%m/%Y'))
#plt.gca().xaxis.set_major_locator(mdates.DayLocator())
#plt.plot(dates[1:500], closing[1:500])
delta = end_date - start_date
a = list(range(0, delta.days))
#print(closing)
plt.plot(range(0, len(closing)), closing)

#plt.gcf().autofmt.xdate()

plt.show()