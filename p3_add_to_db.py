#!/usr/bin/python
from multiprocessing import Process
from pymongo import Connection
import hashlib
import time 
import glob
import syslog
from functions import wikicount
import os

syslog.syslog('p3_add_to_db.py: starting...')
DAY,MONTH,YEAR,HOUR,expiretime=wikicount.fnReturnTimes()
HOUR=wikicount.minusHour(int(HOUR))
DAY,MONTH,HOUR=wikicount.fnFormatTimes(DAY,MONTH,HOUR)

def UpdateHits(FILEPROC,HOUR,DAY,MONTH,YEAR,LANG):
     if LANG=='en':
	HOURLYDB='hitshourly'
        HOURDAYDB='hitshourlydaily'
        HITSMAPDB='hitsmap'
        HITSDAILY='hitsdaily'
     elif LANG=='ru':
	HOURLYDB='ru_hitshourly'
        HOURDAYDB='ru_hitshourlydaily'
        HITSMAPDB='ru_hitsmap'
        HITSDAILY='ru_hitsdaily'
     connection=Connection()
     db=connection.wc
     RECORDS=0
     DAYKEY=str(YEAR)+"_"+str(MONTH)+"_"+str(DAY)
     for FILENAME in glob.glob(FILEPROC):
	print FILENAME	
	try:
		IFILE2=open(FILENAME,'r')
		for line in IFILE2:
			try:
		          line=line.strip().split()
		          HASH=hashlib.sha1(line[1]).hexdigest()
		          POSTFIND={'_id': HASH}
			  TITLESTRING=line[1].decode('utf-8')

			  db[HOURLYDB].update(POSTFIND,{"$inc":{HOUR:int(line[2])}},upsert=True)
			  db[HOURDAYDB].update(POSTFIND,{"$inc":{HOUR:int(line[2])}},upsert=True)
			  db[HITSMAPDB].update(POSTFIND,{"$set":{'title':TITLESTRING}},upsert=True)
			  db[HITSDAILY].update(POSTFIND,
					{"$inc":
						{DAYKEY: int(line[2])}
					,
					"$set":
						{'title':TITLESTRING}
					}	
					,upsert=True)

		          RECORDS+=1
			except UnicodeDecodeError: 
			  syslog.syslog("p3_add_to_db.py - UnicodeDecodeError")
			  pass
     		IFILE2.close()
		os.remove(FILENAME)
	except (NameError,IOError) as e:
		syslog.syslog("Error in "+str(FILENAME)+", P3_add_to_db.py stopping, "+str(e.strerror)+", errno "+str(e.errno) )
		pass
     FINAL="p3_add_to_db.py: time %s processed a total of %s records." % (time.strftime("%T"),str(RECORDS))
     syslog.syslog(FINAL)



FILEPROC2="/tmp/action/q2_pagecounts.processed."+str(HOUR)
FILEPROC3="/tmp/action/q3_pagecounts.processed."+str(HOUR)
FILEPROC4="/tmp/action/q4_pagecounts.processed."+str(HOUR)
FILEPROC5="/tmp/action/q5_pagecounts.processed."+str(HOUR)

ruFILE1="/tmp/ru_action/q1_pagecounts.ru."+str(HOUR)
ruFILE2="/tmp/ru_action/q2_pagecounts.ru."+str(HOUR)
ruFILE3="/tmp/ru_action/q3_pagecounts.ru."+str(HOUR)
ruFILE4="/tmp/ru_action/q4_pagecounts.ru."+str(HOUR)


#FILEPROC2="/tmp/action/q2_pagecounts.processed.17"
#FILEPROC3="/tmp/action/q3_pagecounts.processed.17"
#FILEPROC4="/tmp/action/q4_pagecounts.processed.17"
#FILEPROC5="/tmp/action/q5_pagecounts.processed.17"


if __name__ == '__main__':
    STARTTIME=wikicount.fnStartTimer()
    wikicount.fnSetStatusMsg('p3_add_to_db',0)
    p = Process(target=UpdateHits, args=(FILEPROC2,HOUR,DAY,MONTH,YEAR,'en'))
    q = Process(target=UpdateHits, args=(FILEPROC3,HOUR,DAY,MONTH,YEAR,'en'))
    r = Process(target=UpdateHits, args=(FILEPROC4,HOUR,DAY,MONTH,YEAR,'en'))
    s = Process(target=UpdateHits, args=(FILEPROC5,HOUR,DAY,MONTH,YEAR,'en'))
    p.daemon=True
    q.daemon=True
    r.daemon=True
    s.daemon=True
    p.start()
    q.start()
    r.start()
    s.start()



    p.join()
    q.join()
    r.join()
    s.join()
    
    RUNTIME=wikicount.fnEndTimerCalcRuntime(STARTTIME)
    syslog.syslog('p3_add: EN records added in '+str(RUNTIME)+' seconds. Russian next.')

    STARTTIME=wikicount.fnStartTimer()
    t = Process(target=UpdateHits, args=(ruFILE1,HOUR,DAY,MONTH,YEAR,'ru'))
    u = Process(target=UpdateHits, args=(ruFILE2,HOUR,DAY,MONTH,YEAR,'ru'))
    v = Process(target=UpdateHits, args=(ruFILE3,HOUR,DAY,MONTH,YEAR,'ru'))
    w = Process(target=UpdateHits, args=(ruFILE4,HOUR,DAY,MONTH,YEAR,'ru'))

    t.daemon=True
    u.daemon=True
    v.daemon=True
    w.daemon=True

    t.start()
    u.start()
    v.start()
    w.start()

    t.join()
    u.join()
    v.join()
    w.join()

    RUNTIME=wikicount.fnEndTimerCalcRuntime(STARTTIME)


    syslog.syslog('p3_add: Russian records added in '+str(RUNTIME)+' seconds.P3 Done now!')
    wikicount.fnSetStatusMsg('p3_add_to_db',3)
    wikicount.fnLaunchNextJob('p3_add_to_db')

