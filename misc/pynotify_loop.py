import pyinotify

wm = pyinotify.WatchManager()

notifier = pyinotify.Notifier(wm)
wm.add_watch('/home/anshul/rsp', pyinotify.ALL_EVENTS)
notifier.loop()
