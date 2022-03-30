from stocks.models import Stock
from django.db import models
from datetime import datetime


class Listing(models.Model):
    date = models.DateTimeField()
    opening = models.DecimalField(max_digits=11, decimal_places=5)
    high = models.DecimalField(max_digits=11, decimal_places=5)
    low = models.DecimalField(max_digits=11, decimal_places=5)
    closing = models.DecimalField(max_digits=11, decimal_places=5)
    wap = models.DecimalField(max_digits=20, decimal_places=11)
    traded = models.BigIntegerField()
    trades = models.BigIntegerField()
    turnover = models.BigIntegerField()
    deliverable = models.BigIntegerField()
    stock = models.ForeignKey(Stock,
                              null=False, 
                              on_delete = models.CASCADE)

    def __str__(self):
        return (self.stock.name+' '+ self.date.strftime("%d-%m-%Y"))
    
    class Meta:
        indexes = [
            models.Index(fields=['date'], name='date_idx'),
        ]
