import os
from django.conf import settings
from django.apps import apps

conf = {
        'INSTALLED_APPS': [
            'stocks',
            'django.contrib.contenttypes',
        ],
        'DATABASES': {
            'default': {
            'ENGINE': 'django.db.backends.sqlite3',
            'NAME': os.path.join(os.path.dirname(os.path.abspath(__file__)), 'bsedata.db'),
            },
        }
    }
DEFAULT_AUTO_FIELD = 'django.db.models.AutoField'

settings.configure(**conf)
apps.populate(settings.INSTALLED_APPS)

project_dirs = {
    'reports': os.path.join(os.path.dirname(os.path.abspath(__file__)), 'reports/'),
    'cache'  : os.path.join(os.path.dirname(os.path.abspath(__file__)), 'cache/'),
}

