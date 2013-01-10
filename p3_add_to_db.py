#!/usr/bin/python
from multiprocessing import Process
from pymongo import Connection
import hashlib
import time 
import glob
import syslog
from functions import wikicount

DAY,MONTH,YEAR,HOUR,expiretime=wikicount.fnReturnTimes()
HOUR=int(HOUR)-2
HOUR=wikicount.adjustHour(HOUR)
DAY,MONTH,HOUR=wikicount.fnFormatTimes(DAY,MONTH,HOUR)

def f(FILEPROC,HOUR):
     connection=Connection()
     db=connection.wc
     RECORDS=0
     for FILENAME in glob.glob(FILEPROC+"*"):	
	try:
		IFILE2=open(FILENAME,'r')
		for line in IFILE2:
			try:
			  rec2=line
		          record=line.strip().split()
		          HASH=hashlib.sha1(record[1]).hexdigest()
		          POSTFIND={'_id': HASH}
			  POSTDATE=time.strftime("%D_%H")
			  TITLESTRING=record[1].decode('utf-8')
			  TITLEPOST={'_id':HASH,'title':TITLESTRING}
		          db.hits.update(POSTFIND,{ "$inc" : { "Hits" : int(record[2]) } },upsert=True)
			  db.hitshourly.update(POSTFIND,{"$set":{HOUR:int(record[2])}},upsert=True)
		  	  db.map.update(TITLEPOST,upsert=True)
		          RECORDS+=1
			except UnicodeDecodeError: 
			  syslog.syslog("p3_add_to_db.py - UnicodeDecodeError")
			  pass
     		IFILE2.close()
	except (NameError,IOError):
		syslog.syslog("Error encountered! P3_add_to_db.py stopping, NameError or IOError")
		pass
     FINAL=" time %s processed a total of %s records." % (time.strftime("%T"),str(RECORDS))
     syslog.syslog(FINAL)


FILEPROC2="/tmp/action/q2_pagecounts.processed."+str(HOUR)
FILEPROC3="/tmp/action/q3_pagecounts.processed."+str(HOUR)
FILEPROC4="/tmp/action/q4_pagecounts.processed."+str(HOUR)
FILEPROC5="/tmp/action/q5_pagecounts.processed."+str(HOUR)
FILEPROC6="/tmp/action/q6_pagecounts.processed."+str(HOUR)
FILEPROC7="/tmp/action/q7_pagecounts.processed."+str(HOUR)
FILEPROC8="/tmp/action/q8_pagecounts.processed."+str(HOUR)



#FILEPROC2="/tmp/action/q2_pagecounts.processed.01"
#FILEPROC3="/tmp/action/q3_pagecounts.processed.01"
#FILEPROC4="/tmp/action/q4_pagecounts.processed.01"
#FILEPROC5="/tmp/action/q5_pagecounts.processed.01"
#FILEPROC6="/tmp/action/q6_pagecounts.processed.01"
#FILEPROC7="/tmp/action/q7_pagecounts.processed.01"
#FILEPROC8="/tmp/action/q8_pagecounts.processed.01"


if __name__ == '__main__':
    p = Process(target=f, args=(FILEPROC2,HOUR))
    q = Process(target=f, args=(FILEPROC3,HOUR))
    r = Process(target=f, args=(FILEPROC4,HOUR))
    s = Process(target=f, args=(FILEPROC5,HOUR))
    t = Process(target=f, args=(FILEPROC6,HOUR))
    u = Process(target=f, args=(FILEPROC7,HOUR))
    v = Process(target=f, args=(FILEPROC8,HOUR))
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
    v.start()
#    w.start()



    p.join()
    q.join()
    r.join()
    s.join()
    t.join()
    u.join()
    v.join()
#    w.join()

