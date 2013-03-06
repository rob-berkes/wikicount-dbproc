#!/usr/bin/python
import os
import gzip 
import time 
from functions import wikicount
from time import time
import syslog

a=time()
syslog.syslog("p1_split.py: starting...")
FILEBASE="/tmp/staging/pagecounts.tmp"
DAY,MONTH,YEAR,HOUR,expiretime=wikicount.fnReturnTimes()
HOUR=wikicount.minusHour(int(HOUR))
DAY,MONTH,HOUR=wikicount.fnFormatTimes(DAY,MONTH,HOUR)
wikicount.fnSetStatusMsg('p1_split',0)

FILEPROC2="/tmp/staging/q2_pagecounts.processed."+str(HOUR)
FILEPROC3="/tmp/staging/q3_pagecounts.processed."+str(HOUR)
FILEPROC4="/tmp/staging/q4_pagecounts.processed."+str(HOUR)
FILEPROC5="/tmp/staging/q5_pagecounts.processed."+str(HOUR)


IFILE=gzip.open(FILEBASE,"r")
OFILE2=open(FILEPROC2,"w")
OFILE3=open(FILEPROC3,"w")
OFILE4=open(FILEPROC4,"w")
OFILE5=open(FILEPROC5,"w")
NUMBERLOGFILES=4
COUNTTHRESHOLD=4
RECCOUNT=0


for line in IFILE:
        record=line.strip().split()
	RECCOUNT+=1
	try:
		if record[0]=="en" and int(record[2])>COUNTTHRESHOLD and RECCOUNT % NUMBERLOGFILES == 0:
			OFILE2.write(str(line))
	 	elif record[0]=="en" and int(record[2])>COUNTTHRESHOLD and RECCOUNT % NUMBERLOGFILES == 1:
			OFILE3.write(str(line))
	        elif record[0]=="en" and int(record[2])>COUNTTHRESHOLD and RECCOUNT % NUMBERLOGFILES == 2:
			OFILE4.write(str(line))
	        elif record[0]=="en" and int(record[2])>COUNTTHRESHOLD and RECCOUNT % NUMBERLOGFILES == 3:
			OFILE5.write(str(line))
	except ValueError:
		pass
OFILE2.close()
OFILE3.close()
OFILE4.close()
OFILE5.close()

os.remove(FILEBASE)
b=time()
c=b-a
d=round(c,3)
syslog.syslog("p1_split.py : runtime "+str(d)+" seconds.")
wikicount.fnSetStatusMsg('p1_split',3)
wikicount.fnLaunchNextJob('p1_split')
