#!/usr/bin/python
import urllib2
import os
from functions import wikicount
import syslog

FILEBASE="/tmp/staging/pagecounts.tmp"
URL="http://dumps.wikimedia.org/other/pagecounts-raw/2012/2012-11/pagecounts-"

DAY,MONTH,YEAR,HOUR,expiretime=wikicount.fnReturnTimes()
HOUR=wikicount.minusHour(int(HOUR))
HOUR-=3
if HOUR==-1:
	HOUR=23
elif HOUR==-2:
	HOUR=22
elif HOUR==-3:
	HOUR=21
DAY,MONTH,HOUR=wikicount.fnFormatTimes(DAY,MONTH,HOUR)
URL="http://dumps.wikimedia.your.org/other/pagecounts-raw/"+str(YEAR)+"/"+str(YEAR)+"-"+str(MONTH)+"/pagecounts-"
#print HOUR
URLDATE=str(YEAR)+str(MONTH)+str(DAY)
URLSUFFIX="-"+str(HOUR)+"0000.gz"
URL+=URLDATE
URL+=URLSUFFIX

print URL
#Now with URL, download file
syslog.syslog("[p0-dl.py] - starting download"+str(URL))
COUNTFILE=urllib2.urlopen(URL)
OFILE=open(FILEBASE,"w")
OFILE.write(COUNTFILE.read())
OFILE.close()
syslog.syslog("[p0-dl.py] - finished download"+str(URL))

