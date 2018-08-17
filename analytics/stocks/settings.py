import os
from django.conf import settings
from django.apps import apps

conf = {
        'INSTALLED_APPS': [
            'stocks'
        ],
        'DATABASES': {
            'default': {
            'ENGINE': 'django.db.backends.sqlite3',
            'NAME': os.path.join(os.path.dirname(os.path.abspath(__file__)), 'bsedata.db'),
            },
        }
    }

settings.configure(**conf)
apps.populate(settings.INSTALLED_APPS)

