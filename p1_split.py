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

LANGLIST=wikicount.getLanguageList()
for line in IFILE:
        record=line.strip().split()
	RECCOUNT+=1
	try:
		if record[0] in LANGLIST and int(record[2])>COUNTTHRESHOLD and RECCOUNT % NUMBERLOGFILES==0:
			fName='/tmp/'+str(record[0])+'_staging/q1_pagecounts.'+str(HOUR)
			fFILE=open(fName,"a")
			fFILE.write(str(line))
			fFILE.close()
	        elif record[0] in LANGLIST and int(record[2])>COUNTTHRESHOLD and RECCOUNT % NUMBERLOGFILES == 1:
			fName='/tmp/'+str(record[0])+'_staging/q2_pagecounts.'+str(HOUR)
			fFILE=open(fName,"a")
			fFILE.write(str(line))
			fFILE.close()
	        elif record[0] in LANGLIST and int(record[2])>COUNTTHRESHOLD and RECCOUNT % NUMBERLOGFILES == 2:
			fName='/tmp/'+str(record[0])+'_staging/q3_pagecounts.'+str(HOUR)
			fFILE=open(fName,"a")
			fFILE.write(str(line))
			fFILE.close()
	        elif record[0] in LANGLIST and int(record[2])>COUNTTHRESHOLD and RECCOUNT % NUMBERLOGFILES == 3:
			fName='/tmp/'+str(record[0])+'_staging/q4_pagecounts.'+str(HOUR)
			fFILE=open(fName,"a")
			fFILE.write(str(line))
			fFILE.close()	

	except ValueError:
		pass



os.remove(FILEBASE)
b=time()
c=b-a
d=round(c,3)
syslog.syslog("p1_split.py : runtime "+str(d)+" seconds.")
wikicount.fnSetStatusMsg('p1_split',3)
wikicount.fnLaunchNextJob('p1_split')
