from libnmap.process import NmapProcess
from libnmap.parser import NmapParser, NmapParserException

from pexpect import pxssh

nm = NmapProcess('192.168.0.0/16', options='-sn')
rc = nm.run()

known_hosts = []
fd = open('result.txt', 'r')
for line in fd:
  known_hosts.append(line.rstrip())

active_hosts = []
vulnerable_hosts = []
if nm.rc != 0:
  print nm.stderr
else:
  report = NmapParser.parse(nm.stdout)
  for host in report.hosts:
    if host not in known_hosts:
      if host.status == 'up':
        if len(host.hostnames):
          tmp_host = host.hostnames.pop()
        else:
          tmp_host = host.address
        active_hosts.append(tmp_host)

for host in active_hosts:
  #Try to ssh to this device using 'user':'user123'
  #Note that this may cause an openssh GUI prompt to enter password.
  # I worked around this by using the script in the tty (Ctrl+Shift+F1)
  ssh = pxssh.pxssh()
  ssh.force_password = True
  try: 
   ssh.login(host, 'user', password='user123')
   ssh.logout()
   vulnerable_hosts.append(host)
   print host
  except pxssh.ExceptionPxssh:
   pass
  except:
   pass
  
print '{vul}/{tot} {percent} use vulnerable'.format(vul=len(vulnerable_hosts), tot=len(active_hosts), percent=len(vulnerable_hosts)*1.0/len(active_hosts))
