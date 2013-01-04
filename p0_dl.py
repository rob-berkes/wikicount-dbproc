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
from functions import wikicount

FILEBASE="/tmp/staging/pagecounts.tmp"
URL="http://dumps.wikimedia.org/other/pagecounts-raw/2012/2012-11/pagecounts-"

DAY,MONTH,YEAR,HOUR,expiretime=wikicount.fnReturnTimes()
DAY,MONTH,HOUR=wikicount.fnFormatTimes(DAY,MONTH,HOUR)

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

