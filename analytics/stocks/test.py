import os
import settings

from stocks.models import Listing, Industry, Stock

a = Industry(name="Something")
print(a)
a.save()
