#!/usr/bin/python
import urllib2
import os
from functions import wikicount
from time import time
import syslog
import glob

FILEBASE="/tmp/staging/pagecounts.tmp"
wikicount.fnSetStatusMsg('p0_dl',0)
DAY,MONTH,YEAR,HOUR,expiretime=wikicount.fnReturnTimes()
#HOUR=wikicount.minusHour(int(HOUR))
#HOUR=wikicount.minusHour(int(HOUR))
DAY,MONTH,HOUR=wikicount.fnFormatTimes(DAY,MONTH,HOUR)
URL="http://dumps.wikimedia.org/other/pagecounts-raw/"+str(YEAR)+"/"+str(YEAR)+"-"+str(MONTH)+"/pagecounts-"
#print HOUR
URLDATE=str(YEAR)+str(MONTH)+str(DAY)
URLSUFFIX="-"+str(HOUR)+"*.gz"
URL+=URLDATE
URL+=URLSUFFIX

#Now with URL, download file
a=time()
syslog.syslog("[p0-dl.py] - starting download"+str(URL))
missing=True
MINUTESEARCH=0
while missing and MINUTESEARCH<60:
	MINUTE=wikicount.fnFormatHour(MINUTESEARCH)
	URL="http://dumps.wikimedia.org/other/pagecounts-raw/"+str(YEAR)+"/"+str(YEAR)+"-"+str(MONTH)+"/pagecounts-"
	#print HOUR
	URLDATE=str(YEAR)+str(MONTH)+str(DAY)
	URLSUFFIX="-"+str(HOUR)+"00"+str(MINUTE)+".gz"
	URL+=URLDATE
	URL+=URLSUFFIX
	MINUTESEARCH+=1	
	try:	
		print URL
		COUNTFILE=urllib2.urlopen(URL)
		if COUNTFILE.code==200:
			fetchURL=URL
			missing=False
	except urllib2.HTTPError:
		pass
if not missing:
	COUNTFILE=urllib2.urlopen(fetchURL)
	OFILE=open(FILEBASE,"w")
	OFILE.write(COUNTFILE.read())
	OFILE.close()
	b=time()
	c=b-a
	d=round(c,3)
	syslog.syslog("[p0-dl.py] - finished download "+str(URL)+" in "+str(d)+" seconds.")
else:
	syslog.syslog("[p0-dl.py] - download not found, 404 error")
wikicount.fnSetStatusMsg('p0_dl',3)
wikicount.fnLaunchNextJob('p0_dl')
