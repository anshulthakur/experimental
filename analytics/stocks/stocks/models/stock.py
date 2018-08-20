from stocks.models import Industry
from django.db import models


class Stock(models.Model):
    security = models.BigIntegerField()
    sid = models.CharField(blank=False, 
                            null=False,
                            max_length=15)
    name = models.CharField(blank=False, 
                            null=False,
                            max_length=255)
    group = models.CharField(blank=True,
                             default='',
                             max_length=5)
    face_value = models.DecimalField(max_digits=10, decimal_places = 4)
    isin = models.CharField(blank=False, 
                            null=False,
                            max_length=15)
    industry = models.ForeignKey(Industry, 
                                 null=True,
                                 default=None,
                                 on_delete = models.SET_NULL)
                                 
    def __str__(self):
        return (self.name)

    def get_quote_url(self):
        return ("http://www.bseindia.com/markets/equity/EQReports/StockPrcHistori.aspx?expandable=7&scripcode=" + str(self.security) + "&flag=sp&Submit=G")
