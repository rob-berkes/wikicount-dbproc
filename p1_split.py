#!/usr/bin/python
import os
import gzip 
import time 
from functions import wikicount

FILEBASE="/tmp/staging/pagecounts.tmp"
DAY,MONTH,YEAR,HOUR,expiretime=wikicount.fnReturnTimes()
HOUR=wikicount.minusHour(int(HOUR))
DAY,MONTH,HOUR=wikicount.fnFormatTimes(DAY,MONTH,HOUR)
print HOUR
FILEPROC2="/tmp/staging/q2_pagecounts.processed."+str(HOUR)
FILEPROC3="/tmp/staging/q3_pagecounts.processed."+str(HOUR)
FILEPROC4="/tmp/staging/q4_pagecounts.processed."+str(HOUR)
FILEPROC5="/tmp/staging/q5_pagecounts.processed."+str(HOUR)
FILEPROC6="/tmp/staging/q6_pagecounts.processed."+str(HOUR)
FILEPROC7="/tmp/staging/q7_pagecounts.processed."+str(HOUR)
FILEPROC8="/tmp/staging/q8_pagecounts.processed."+str(HOUR)
#FILEPROC9="/tmp/staging/q9_pagecounts.processed."+str(HOUR)


IFILE=gzip.open(FILEBASE,"r")
OFILE2=open(FILEPROC2,"w")
OFILE3=open(FILEPROC3,"w")
OFILE4=open(FILEPROC4,"w")
OFILE5=open(FILEPROC5,"w")
OFILE6=open(FILEPROC6,"w")
OFILE7=open(FILEPROC7,"w")
OFILE8=open(FILEPROC8,"w")
#OFILE9=open(FILEPROC9,"w")
NUMBERLOGFILES=7
COUNTTHRESHOLD=5
RECCOUNT=0
for line in IFILE:
        record=line.strip().split()
	RECCOUNT+=1
	if record[0]=="en" and int(record[2])>COUNTTHRESHOLD and RECCOUNT % NUMBERLOGFILES == 0:
		OFILE2.write(str(line))
        elif record[0]=="en" and int(record[2])>COUNTTHRESHOLD and RECCOUNT % NUMBERLOGFILES == 1:
		OFILE3.write(str(line))
        elif record[0]=="en" and int(record[2])>COUNTTHRESHOLD and RECCOUNT % NUMBERLOGFILES == 2:
		OFILE4.write(str(line))
        elif record[0]=="en" and int(record[2])>COUNTTHRESHOLD and RECCOUNT % NUMBERLOGFILES == 3:
		OFILE5.write(str(line))
        elif record[0]=="en" and int(record[2])>COUNTTHRESHOLD and RECCOUNT % NUMBERLOGFILES == 4:
		OFILE6.write(str(line))
        elif record[0]=="en" and int(record[2])>COUNTTHRESHOLD and RECCOUNT % NUMBERLOGFILES == 5:
		OFILE7.write(str(line))
        elif record[0]=="en" and int(record[2])>COUNTTHRESHOLD and RECCOUNT % NUMBERLOGFILES == 6:
		OFILE8.write(str(line))
#        elif record[0]=="en" and int(record[2])>COUNTTHRESHOLD and RECCOUNT % NUMBERLOGFILES == 7:
#		OFILE8.write(str(line))
 #       elif record[0]=="en" and int(record[2])>COUNTTHRESHOLD and RECCOUNT % NUMBERLOGFILES == 8:
#		OFILE9.write(str(line))
OFILE2.close()
OFILE3.close()
OFILE4.close()
OFILE5.close()
OFILE6.close()
OFILE7.close()
OFILE8.close()
#OFILE9.close()

os.remove(FILEBASE)

