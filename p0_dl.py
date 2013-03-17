#!/usr/bin/python
import urllib2
import os
from functions import wikicount
from time import time
import syslog

FILEBASE="/tmp/staging/pagecounts.tmp"
wikicount.fnSetStatusMsg('p0_dl',0)
DAY,MONTH,YEAR,HOUR,expiretime=wikicount.fnReturnTimes()
HOUR=wikicount.minusHour(int(HOUR))
DAY,MONTH,HOUR=wikicount.fnFormatTimes(DAY,MONTH,HOUR)
URL="http://dumps.wikimedia.org/other/pagecounts-raw/"+str(YEAR)+"/"+str(YEAR)+"-"+str(MONTH)+"/pagecounts-"
#print HOUR
URLDATE=str(YEAR)+str(MONTH)+str(DAY)
URLSUFFIX="-"+str(HOUR)+"0000.gz"
URL+=URLDATE
URL+=URLSUFFIX

print URL
#Now with URL, download file
a=time()
syslog.syslog("[p0-dl.py] - starting download"+str(URL))
COUNTFILE=urllib2.urlopen(URL)
OFILE=open(FILEBASE,"w")
OFILE.write(COUNTFILE.read())
OFILE.close()
b=time()
c=b-a
d=round(c,3)
syslog.syslog("[p0-dl.py] - finished download "+str(URL)+" in "+str(d)+" seconds.")

wikicount.fnSetStatusMsg('p0_dl',3)
wikicount.fnLaunchNextJob('p0_dl')
