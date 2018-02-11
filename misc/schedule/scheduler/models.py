#Maximum levels of priority 
MIN_PRIORITY = 100

#Types of Workers
MACHINE = 0
HUMAN   = 1

#Types of Schedulers:
#Most of the schedulers will take a time frame as input while calculating the schedule.
BEST_FIT = 0 #Find the most optimal slot
FIRST_FIT = 1 #Find the first fitting slot and fragment time chunk
PRIORITY = 2  #Schedule things according to priority
LEAST_TIME_FIRST = 3 #Schedule shortest duration tasks first
LEAST_EFFORT_FIRST = 4 #Schedule least effort tasks first


def check_empty(name, field):
  if field is None:
    raise Exception('{n} must not be empty'.format(n=name))

class Task(object):
  def __init__(self, name, descr=None, priority=MIN_PRIORITY, start_time=None, end_time=None, duration=0, category=None, effort=0,parent=None, *args, **kwargs):
    check_empty('name', name)
    self.name = name
    self.descr = descr
    self.priority = priority
    self.start_time = start_time
    self.end_time = end_time   
    self.duration  = duration
    self.category = category
    self.expected_effort = effort
    self.parent = parent


class Worker(object):
  def __init__(self, name, entity_type=MACHINE, group = None, *args, **kwargs):
    check_empty('name', name)
    self.name = name
    self.type = entity_type
    self.group = group
    
class Scheduler(object):
  def __init__(self, scheduler_type=BEST_FIT)
