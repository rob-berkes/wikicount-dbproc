#/usr/bin/python
import glob
import os 
import string
from functions import wikicount
wikicount.fnSetStatusMsg('p2x_move_to_action',0)
DAY,MONTH,YEAR,HOUR,expiretime=wikicount.fnReturnTimes()
HOUR=int(HOUR)-2
if HOUR==-1:
	HOUR=23
DAY,MONTH,HOUR=wikicount.fnFormatTimes(DAY,MONTH,HOUR)

for FILENAME in glob.glob('/tmp/ondeck/q*'):
	IFILE=open(FILENAME,'r')
	OFILENAME=string.replace(FILENAME,'ondeck','action')
	OFILE=open(OFILENAME,'w')
	for line in IFILE: 
		OFILE.write(line)
	IFILE.close()
	OFILE.close() 
	os.remove(FILENAME)
wikicount.fnSetStatusMsg('p2x_move_to_action',3)
wikicount.fnLaunchNextJob('p2x_move_to_action')
