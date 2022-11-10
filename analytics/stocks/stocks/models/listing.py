from stocks.models import Stock,Market
from django.db import models


class Listing(models.Model):
    date = models.DateTimeField()
    open = models.DecimalField(max_digits=11, decimal_places=5)
    high = models.DecimalField(max_digits=11, decimal_places=5)
    low = models.DecimalField(max_digits=11, decimal_places=5)
    close = models.DecimalField(max_digits=11, decimal_places=5)
    traded = models.BigIntegerField(null=True)
    trades = models.BigIntegerField(null=True)
    deliverable = models.BigIntegerField(null=True)
    stock = models.ForeignKey(Stock,
                              null=False, 
                              on_delete = models.CASCADE)

    def __str__(self):
        return (self.stock.name+' '+ self.date.strftime("%d-%m-%Y"))
    
    class Meta:
        indexes = [
            models.Index(fields=['date'], name='date_idx'),
        ]
