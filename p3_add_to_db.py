#!/usr/bin/python
from multiprocessing import Process
from datetime import date
from pymongo import Connection
import gzip
import hashlib
import random
import time 
import glob
import syslog
from functions import wikicount

DAY,MONTH,YEAR,HOUR,expiretime=wikicount.fnReturnTimes()
HOUR=int(HOUR)-1
if HOUR==-1:
	HOUR=23
DAY,MONTH,HOUR=wikicount.fnFormatTimes(DAY,MONTH,HOUR)

def f(FILEPROC):
     connection=Connection()
     CONNID=random.randint(1,500)
     HOUR=time.strftime("%H")
     db=connection.wc
     db.loghits.insert({'time':time.strftime("%T"),'date':time.strftime("%D"),'text':"Connection, <"+str(CONNID)+"> started @ "+time.strftime("%T")+" , "+time.strftime("%D")+". Thread starting."})
     RECORDS=0
     for FILENAME in glob.glob(FILEPROC+"*"):	
	try:
		IFILE2=open(FILENAME,'r')
		for line in IFILE2:
			try:
			  rec2=line
		          record=line.strip().split()
		          HASH=hashlib.sha1(record[1]).hexdigest()
			  POSTHOURLY={'_id':HASH,HOUR:int(record[2])}
		          POSTFIND={'_id': HASH}
			  POSTDATE=time.strftime("%D_%H")
		   	  FIELDHITS=POSTDATE+"_hits"
			  FIELDTRAF=POSTDATE+"_traf"		
		          db.hits.update(POSTFIND,{ "$inc" : { "Hits" : int(record[2]) } },upsert=True)
			  db.hitshourly.update(POSTFIND,{"$set":{HOUR:int(record[2])}},upsert=True)
#			  db.hitsperiod.update(POSTFIND,{"$set":{str(FIELDHITS):int(record[2])}},upsert=True)
#			  db.sizeperiod.update(POSTFIND,{'$set':{str(FIELDTRAF):int(record[3])}},upsert=True)
			 #db.hits.insert(POSTNEW)
		          RECORDS+=1
#		          if (RECORDS % 150000) == 0 :
#		               OUT= str(CONNID)+" proc'd record: %s \n" % (str(RECORDS))
#		               POSTOUT={'time':time.strftime("%T"),'text':OUT}
#		               db.loghits.insert(POSTOUT)
			except UnicodeDecodeError: 
			  syslog.syslog("p3_add_to_db.py - UnicodeDecodeError")
			  pass
     		IFILE2.close()
	except (NameError,IOError):
		syslog.syslog("Error encountered! P3_add_to_db.py stopping, NameError or IOError")
		pass

     FINAL=str(CONNID)+" time %s processed a total of %s records." % (time.strftime("%T"),str(RECORDS))
     POSTFINAL={'time':time.strftime("%T"),'date':time.strftime("%D"),'text':FINAL}
     db.loghits.insert(POSTFINAL)


FILEPROC2="/tmp/action/q2_pagecounts.processed."+str(HOUR)
FILEPROC3="/tmp/action/q3_pagecounts.processed."+str(HOUR)
FILEPROC4="/tmp/action/q4_pagecounts.processed."+str(HOUR)
FILEPROC5="/tmp/action/q5_pagecounts.processed."+str(HOUR)
FILEPROC6="/tmp/action/q6_pagecounts.processed."+str(HOUR)
FILEPROC7="/tmp/action/q7_pagecounts.processed."+str(HOUR)
FILEPROC8="/tmp/action/q8_pagecounts.processed."+str(HOUR)
#FILEPROC9="/tmp/action/q9_pagecounts.processed."





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

