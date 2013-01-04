#!/usr/bin/python
from multiprocessing import Process
from datetime import date
from pymongo import Connection
import time
import urllib2
import gzip
import hashlib
import glob
import os

def f(FILEPROC):
     connection=Connection()
     db=connection.wc
     db.logmap.insert({'time':time.strftime("%T"),'date':time.strftime("%D"),'text':"Time: "+time.strftime("%T")+", "+time.strftime("%D")+". Launched new proc"})
     RECORDS=0
     for FILENAME in glob.glob(str(FILEPROC)+"*"):
	print FILENAME
	IFILE2=open(FILENAME,'r')
	
   	for line in IFILE2:
	  	 record=line.strip().split()
	         HASH=hashlib.sha1(record[1]).hexdigest()
	         POSTFIND={'_id': HASH}
		 TITLESTRING=record[1].decode('utf-8')
	         db.map.update(POSTFIND,{ "$inc" : { 'bwidth' : int(record[3]) },"$set":{'title':TITLESTRING} },upsert=True)
	         RECORDS+=1
	IFILE2.close()
	os.remove(FILENAME)
     FINAL="Total of "+str(RECORDS)+" records processed, finished at "+time.strftime("%T")
     POSTFINAL={'time':time.strftime("%T"),'date':time.strftime("%D"),'text':FINAL}
     db.logmap.insert(POSTFINAL)
	

FILEBASE="/tmp/pagecounts.tmp"
FILEPROC2="/tmp/action/q2_pagecounts.processed."
FILEPROC3="/tmp/action/q3_pagecounts.processed."
FILEPROC4="/tmp/action/q4_pagecounts.processed."
FILEPROC5="/tmp/action/q5_pagecounts.processed."
FILEPROC6="/tmp/action/q6_pagecounts.processed."
FILEPROC7="/tmp/action/q7_pagecounts.processed."
FILEPROC8="/tmp/action/q8_pagecounts.processed."
#FILEPROC9="/tmp/action/q9_pagecounts.processed."
URL="http://dumps.wikimedia.org/other/pagecounts-raw/2012/2012-11/pagecounts-"

d=date.today()
YEAR=d.year
MONTH=d.month
DAY=d.day
HOUR=time.strftime('%H')
HOUR=int(HOUR)-1
HOUR='%02d' % (HOUR,)
URLDATE=d.strftime("%Y%m%d")
URLSUFFIX="-"+str(HOUR)+"0000.gz"
URL+=URLDATE
URL+=URLSUFFIX
HITS=0






if __name__ == '__main__':
    p = Process(target=f, args=(FILEPROC2,))
    q = Process(target=f, args=(FILEPROC3,))
    r = Process(target=f, args=(FILEPROC4,))
    s = Process(target=f, args=(FILEPROC5,))
    t = Process(target=f, args=(FILEPROC6,))
    u = Process(target=f, args=(FILEPROC7,))
    v = Process(target=f, args=(FILEPROC8,))
#    w = Process(target=f, args=(FILEPROC9,))
    p.daemon=True
    q.daemon=True
    r.daemon=True
    s.daemon=True
    t.daemon=True
    u.daemon=True
    v.daemon=True
#    w.daemon=True
    p.start()
    q.start()
    r.start()
    s.start()
    t.start()
    u.start()
#    w.start()
    v.start()



    p.join()
    q.join()
    r.join()
    s.join()
    t.join()
    u.join()
    v.join()
#    w.join()

