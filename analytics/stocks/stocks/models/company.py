from django.db import models
from stocks.models import Industry

class Company(models.Model):
    name = models.CharField(blank=False, 
                            null=False,
                            max_length=255)
    isin = models.CharField(blank=False,
                            null=False,
                            max_length=15)
    def __str__(self):
        return self.name

    class Meta:
        indexes = [
            models.Index(fields=['name'], name='company_idx'),
            models.Index(fields=['isin'], name='isin_idx'),
        ]