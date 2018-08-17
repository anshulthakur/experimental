from django.db import models

class Industry(models.Model):
    name = models.CharField(blank=False, 
                            null=False,
                            max_length=255)
    
    def __str__(self):
        return self.name

