import csv
import sys

if len(sys.argv) < 2:
	print "Insufficient Parameters"
	print "Usage: %s <csv file of equity data>" %sys.argv[0]
	exit()

with open('bsedata/'+sys.argv[1]) as fr, open('bsedata_new/'+sys.argv[1],"wb") as fw:
    cr = csv.reader(fr,delimiter=",")
    cw = csv.writer(fw,delimiter=",")
    cw.writerow(next(cr))  # write title as-is
    cw.writerows(reversed(list(cr)))
