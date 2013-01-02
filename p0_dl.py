#!/usr/bin/python
from datetime import date
from pymongo import Connection
import time
import urllib2
import gzip
import hashlib
import random
import re
import os

FILEBASE="/tmp/staging/pagecounts.tmp"
URL="http://dumps.wikimedia.org/other/pagecounts-raw/2012/2012-11/pagecounts-"

d=date.today()
YEAR=d.year
MONTH=d.month
DAY=d.day
HOUR=time.strftime('%H')
if HOUR=='01':
	HOUR=23
	DAY-=1
elif HOUR=='00':
	HOUR=22
	DAY-=1
else:
	HOUR=int(HOUR)-2
if DAY==0:
	DAY=30
HOUR='%02d' % (HOUR,)
DAY='%02d' % (DAY,)
MONTH='%02d' % (MONTH,)
URL="http://dumps.wikimedia.org/other/pagecounts-raw/"+str(YEAR)+"/"+str(YEAR)+"-"+str(MONTH)+"/pagecounts-"
print HOUR
URLDATE=str(YEAR)+str(MONTH)+str(DAY)
URLSUFFIX="-"+str(HOUR)+"0000.gz"
URL+=URLDATE
URL+=URLSUFFIX
HITS=0
FILEPROC2="/tmp/staging/q2_pagecounts.processed."+str(HOUR)
FILEPROC3="/tmp/staging/q3_pagecounts.processed."+str(HOUR)
FILEPROC4="/tmp/staging/q4_pagecounts.processed."+str(HOUR)
FILEPROC5="/tmp/staging/q5_pagecounts.processed."+str(HOUR)
FILEPROC6="/tmp/staging/q6_pagecounts.processed."+str(HOUR)
FILEPROC7="/tmp/staging/q7_pagecounts.processed."+str(HOUR)


#Now with URL, download file
print "Downloading file: "+URL+" ... "
COUNTFILE=urllib2.urlopen(URL)
OFILE=open(FILEBASE,"w")
OFILE.write(COUNTFILE.read())
OFILE.close()
print "Done! Now processing the file..."

