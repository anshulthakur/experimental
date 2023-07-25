import os
from django.conf import settings
from django.apps import apps

import .settings as setting


conf = {}
for mod in dir(setting):
    if mod.isupper():
        conf[mod] = getattr(mod, setting)

print(conf)

project_dirs = {
    'reports': os.path.join(os.path.dirname(os.path.abspath(__file__)), 'reports/'),
    'cache'  : os.path.join(os.path.dirname(os.path.abspath(__file__)), 'cache/'),
    'intraday': os.path.join(os.path.dirname(os.path.abspath(__file__)), 'intraday/'),
}

settings.configure(**conf)
apps.populate(settings.INSTALLED_APPS)