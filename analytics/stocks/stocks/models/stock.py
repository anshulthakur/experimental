from stocks.models import Market
from django.db import models
from django.contrib.contenttypes.fields import GenericForeignKey
from django.contrib.contenttypes.models import ContentType

from random import randint

class StockManager(models.Manager):
    def random(self):
        count = self.aggregate(count=models.Count('id'))['count']
        random_index = randint(0, count - 1)
        return self.all()[random_index]

class Stock(models.Model):
    symbol = models.CharField(blank=False,
                            null=False,
                            max_length=15)
    group = models.CharField(blank=True,
                             default='',
                             max_length=5)
    face_value = models.DecimalField(max_digits=10, decimal_places = 4)
    sid = models.BigIntegerField(default=None, 
                                 null=True)
    market = models.ForeignKey(Market,
                             null=True,
                             to_field='name',
                             on_delete = models.CASCADE)
    content_type = models.ForeignKey(ContentType, null=True, on_delete=models.SET_NULL)
    object_id = models.PositiveIntegerField(default=None, null=True)
    content_object = GenericForeignKey('content_type', 'object_id')

    objects = StockManager()
    def __str__(self):
        return (self.symbol)

    class Meta:
        indexes = [
            models.Index(fields=['symbol'], name='security_idx'),
            models.Index(fields=['sid'], name='sid_idx'),
            models.Index(fields=["content_type", "object_id"]),
        ]
