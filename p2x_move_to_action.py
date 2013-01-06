#/usr/bin/python
import glob
import os 
import string
from functions import wikicount

DAY,MONTH,YEAR,HOUR,expiretime=wikicount.fnReturnTimes()
HOUR=int(HOUR)-2
if HOUR==-1:
	HOUR=23
DAY,MONTH,HOUR=wikicount.fnFormatTimes(DAY,MONTH,HOUR)

for FILENAME in glob.glob('/tmp/ondeck/q*'+str(HOUR)):
	IFILE=open(FILENAME,'r')
	OFILENAME=string.replace(FILENAME,'ondeck','action')
	OFILE=open(OFILENAME,'w')
	for line in IFILE: 
		OFILE.write(line)
	IFILE.close()
	OFILE.close() 
	os.remove(FILENAME)

