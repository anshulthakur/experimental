#!/usr/bin/python
'''
This script opens a file for taking in activity input from user when invoked during morning half.
Before that is done, it will put the previous feedback into a report file.
In the evening, it will open a feedback file to collect a followup feedback from the user.

The schedule file is kept intact for the week and for a new week, the file is cleared after the 
details are put in the report.
'''
import os
import sys

import datetime 

schedule_filename = "/home/anshul/Dropbox/schedule/schedule.md"
feedback_filename = "/home/anshul/Dropbox/schedule/feedback.txt"
report_file = "/home/anshul/Dropbox/schedule/report.csv"

time = datetime.datetime.now()

import re

def create_schedule():
  '''
  Opens the file @schedule_filename and makes entry for day.
  
  If it is the first working day of the week, its makes entry for the week
  plan also. For that, it looks into the last feedback file date and sees if we
  are on the same week.
  '''
  feedback_fd = open(feedback_filename, 'r')
  query_str = '### Schedule for (\d{1,2})-(\d{1,2})-(\d{4})'
  found = False;
  for line in feedback_fd:
    ret = re.match(query_str,line)
    if found is False and ret is not None:
      #Under normal cases, expect to find it in the first line itself.
      day = int(ret.group(1))
      month = int(ret.group(2))
      year = int(ret.group(3))
      found = True
      break
  feedback_fd.close()
  #If year is the same, just compare if both dates are on the same week
  if(time.year == year):
    if( datetime.date(time.year, time.month, time.day).isocalendar()[1] != datetime.date(year, month, day).isocalendar()[1]):
      fd = open(schedule_filename, 'w') #Write Mode: Overwrite Previous Contents
      fd.write('\n\n***\n### Tentative Schedule for this week\n- ')
    else:
      fd = open(schedule_filename, 'a')
  elif time.weekday() == 0:
    #If New Year's first day is in the middle of the week, just see if we are on 0th weekday
    fd = open(schedule_filename, 'w')
    fd.write('\n\n***\n### Tentative Schedule for this week\n- ')
    
  fd.write('\n\n***\n### Schedule for {day}-{month}-{year}\n- '.format(day=time.day, month=time.month, year=time.year))
  fd.close()


def get_todays_schedule(fd, time=None):
  schedule = ''
  time = datetime.datetime.now() if time is None else time
  found = False
  first_action_item = True
  estimate_time = ''
  for line in fd:
    query_str = re.escape('### Schedule for {day}-{month}-{year}'.format(day=time.day, month=time.month, year=time.year))
    if found is False and re.match(query_str, line) is not None:
      #Found the line, just parse the rest of the document into buffer
      schedule += line
      found = True
    elif found is True:
      if re.match('-', line) is not None:
        #A new action item begins
        if not first_action_item:  
          #Only the first action item must not append this before itself      
          schedule += '\nStarted?:'
          schedule += '\nTime taken[{estimate}]:'.format(estimate=estimate_time)
          schedule += '\nCompleted?:'
          schedule += '\nRemarks:\n'
        else:
          first_action_item = False
        schedule += '\n\nActivity:\n'
      #Try to find an estimated time of completion
      est  = re.findall('\[(\d{0,1}\.\d{1,}|\d{0,})\]?', line)
      if len(est) > 0:
        estimate_time = est[0]
        line = re.sub('\['+est[0]+'\]', '', line)
      schedule += line.strip('-').strip()
  #Add this for last action item
  schedule += '\nStarted?:'
  schedule += '\nTime taken[{estimate}]:'.format(estimate=estimate_time)
  schedule += '\nCompleted?:'
  schedule += '\nRemarks:\n'
  return schedule

def followup_on_schedule(time=None):
  fd_schedule = open(schedule_filename, 'r')
  fd_feedback = open(feedback_filename, 'w')
  schedule = get_todays_schedule(fd_schedule, time=time)
  if schedule == '':
    #We probably did not create a schedule in the morning (was late to office?)
    create_schedule()
    # This is a problem for now. We must launch gedit to let user create schedule and wait until he's done.
  fd_feedback.write(schedule)
  fd_schedule.close()
  fd_feedback.close()

import csv
def update_report():
  state_space = ['activity', 'started', 'time-taken' ,'completed', 'remarks']
  fd_report = open(report_file, 'a')
  try:
    fd_feedback = open(feedback_filename, 'r')
  except IOError:
    #Did I shutdown PC before 4:45pm?
    #Create a mock feedback file with empty feedback
    time = datetime.datetime.now()
    #If I did not make a report on Friday
    if time.weekday() == 0:
      #Case for month ends, Monday is 3rd
      if time.day<=3:
        time.replace(month =time.month -1, day= 30)
  	#Just to be safe, for now. This has several problems, but heck.
      else:
        time.replace(day = time.day -4)
    else:
      time.replace(day = time.day- 1)
    followup_on_schedule(time)
    fd_feedback = open(feedback_filename, 'r')
  writer = csv.writer(fd_report, delimiter=',', quoting=csv.QUOTE_MINIMAL)
  chunk = ['']
  state = None
  date_str = None
  est_time = ''
  for line in fd_feedback:
    #Depending on what state we are in, the commas are to be inserted.
    if state == 'activity':
      #We're writing activity info
      if re.match(re.escape('Started?:'), line) is not None:
        #Activity is complete now. Insert comma
        state = 'started'
        chunk.append('')
        #print state
      else:
        chunk[-1] += line.strip('\n')
    elif state == 'started':
      #We're writing  info
      if re.match('Time taken\[(\d{0,1}\.\d{1,}|\d{0,})\]:', line) is not None:
        #Started information is complete now. Insert comma
        state = 'time-taken'
        est = re.match('.*\[((\d{0,1}\.\d{1,}|\d{0,}))\]', line)
        #We're writing how much time was estimated for this activity today
        if est is not None:
          est_time = est.group(1)
        chunk.append(est_time)
        chunk.append('')
        #print state
      else:
        chunk[-1] += line.strip('\n')
    elif state == 'time-taken':
      if re.match(re.escape('Completed?:'), line) is not None:
        #Time taken information has been taken.
        state = 'completed'
        chunk.append('')
      else:
        chunk[-1] += line.strip('\n')
    elif state == 'completed':
      #We're writing activity info
      if re.match(re.escape('Remarks:'), line) is not None:
        #Activity remarks are being added. Insert comma
        state = 'remarks'
        chunk.append('')
        #print state
      else:
        chunk[-1] += line.strip('\n')
    elif state == 'remarks':
      if re.match(re.escape('Activity:'), line) is None:
        chunk[-1] += line.strip('\n')
      #else a new activity is starting
    if re.match('Activity:', line) is not None and (state is None or state == 'remarks'):
      if state == 'remarks':
        #Write the current chunk into file and reset chunk for next
        #print "Chunk:"
        #print chunk
        writer.writerow(chunk)
        chunk = [''] if date_str is None else [date_str,'']
      state = 'activity'
      #print state
    date = re.match('### Schedule for (\d{1,2}-\d{1,2}-\d{4})', line)
    if date is not None:
       chunk[-1] += date.group(1)
       date_str = date.group(1)
       chunk.append('')
    #print 'Chunk:'
    #print chunk
  #print "Last Chunk:"
  #print chunk
  writer.writerow(chunk)
  fd_feedback.close()
  fd_report.close()  

if time.hour < 12:
  update_report()
  create_schedule()
else:
  followup_on_schedule()

