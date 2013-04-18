#!/usr/bin/python
from multiprocessing import Process
from pymongo import Connection
import hashlib
import time 
import glob
import syslog
from functions import wikicount
import os

STARTTIME=wikicount.fnStartTimer()
syslog.syslog('p3_category_add: starting....')
DAY,MONTH,YEAR,HOUR,expiretime=wikicount.fnReturnTimes()
HOUR=wikicount.minusHour(int(HOUR))
DAY,MONTH,HOUR=wikicount.fnFormatTimes(DAY,MONTH,HOUR)

def UpdateHits(FILEPROC,HOUR,DAY,MONTH,YEAR):
     connection=Connection()
     db=connection.wc
     RECORDS=0
     DAYKEY=str(YEAR)+"_"+str(MONTH)+"_"+str(DAY)
     for FILENAME in glob.glob(FILEPROC+"*"+"."+str(HOUR)):	
	try:
		IFILE2=open(FILENAME,'r')
		for line in IFILE2:
			try:
		          line=line.strip().split()
		          HASH=hashlib.sha1(line[1]).hexdigest()
		          POSTFIND={'_id': HASH}
			  TITLESTRING=line[1].decode('utf-8')
		          db.category.update(POSTFIND,{ "$inc" : { "Hits" : int(line[2]) },"$set":{'title':TITLESTRING} },upsert=True)
			  db.categoryhourly.update(POSTFIND,{"$inc":{HOUR:int(line[2])}},upsert=True)
			  db.categorydaily.update(POSTFIND,{"$inc":{DAYKEY: int(line[2])}},upsert=True)
		          RECORDS+=1
			except UnicodeDecodeError: 
			  syslog.syslog("p3_add_to_db.py - UnicodeDecodeError")
			  pass
     		IFILE2.close()
		os.remove(FILENAME)
	except (NameError,IOError):
		syslog.syslog("Error encountered! P3_add_to_db.py stopping, NameError or IOError")
		pass
     FINAL=" time %s processed a total of %s records." % (time.strftime("%T"),str(RECORDS))
     syslog.syslog(FINAL)


FILEPROC2="/tmp/category/q2_pagecounts.processed.*"
FILEPROC3="/tmp/category/q3_pagecounts.processed.*"
FILEPROC4="/tmp/category/q4_pagecounts.processed.*"
FILEPROC5="/tmp/category/q5_pagecounts.processed.*"
FILEPROC6="/tmp/category/q6_pagecounts.processed.*"
FILEPROC7="/tmp/category/q7_pagecounts.processed.*"
FILEPROC8="/tmp/category/q8_pagecounts.processed.*"





if __name__ == '__main__':
    wikicount.fnSetStatusMsg('p3_category_add',0)
    p = Process(target=UpdateHits, args=(FILEPROC2,HOUR,DAY,MONTH,YEAR))
    q = Process(target=UpdateHits, args=(FILEPROC3,HOUR,DAY,MONTH,YEAR))
    r = Process(target=UpdateHits, args=(FILEPROC4,HOUR,DAY,MONTH,YEAR))
    s = Process(target=UpdateHits, args=(FILEPROC5,HOUR,DAY,MONTH,YEAR))
    t = Process(target=UpdateHits, args=(FILEPROC6,HOUR,DAY,MONTH,YEAR))
    u = Process(target=UpdateHits, args=(FILEPROC7,HOUR,DAY,MONTH,YEAR))
    v = Process(target=UpdateHits, args=(FILEPROC8,HOUR,DAY,MONTH,YEAR))
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
    RUNTIME=wikicount.fnEndTimerCalcRuntime(STARTTIME)
    syslog.syslog('p3_category_add: runtime '+str(RUNTIME)+' seconds.')
    wikicount.fnSetStatusMsg('p3_category_add',3)
    wikicount.fnLaunchNextJob('p3_category_add')
