import os
#os.environ.setdefault("DJANGO_SETTINGS_MODULE", "settings")
from django.conf import settings
from django.apps import apps

import settings as setting
from libinit import is_initialized, initialize

conf = {}
for mod in dir(setting):
    if mod.isupper():
        conf[mod] = getattr(setting, mod)

project_dirs = {
    'reports': os.path.join(os.path.dirname(os.path.abspath(__file__)), 'reports/'),
    'cache'  : os.path.join(os.path.dirname(os.path.abspath(__file__)), 'cache/'),
    'intraday': os.path.join(os.path.dirname(os.path.abspath(__file__)), 'intraday/'),
}

if not is_initialized():
    print("Initializing")
    settings.configure(**conf)
    apps.populate(settings.INSTALLED_APPS)
    initialize()