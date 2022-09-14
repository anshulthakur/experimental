from django.db import models

class Market(models.Model):
    name = models.CharField(blank=False, 
                            null=False,
                            unique=True,
                            max_length=255)
    
    def __str__(self):
        return self.name

