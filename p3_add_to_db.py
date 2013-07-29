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
     HOURLYDB=str(LANG)+'_hitshourly'
     HOURDAYDB=str(LANG)+'_hitshourlydaily'
     HITSMAPDB=str(LANG)+'_hitsmap'
     HITSDAILY=str(LANG)+'_hitsdaily'
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



#FILEPROC2="/tmp/action/q2_pagecounts.processed.17"
#FILEPROC3="/tmp/action/q3_pagecounts.processed.17"
#FILEPROC4="/tmp/action/q4_pagecounts.processed.17"
#FILEPROC5="/tmp/action/q5_pagecounts.processed.17"


if __name__ == '__main__':
    STARTTIME=wikicount.fnStartTimer()
    wikicount.fnSetStatusMsg('p3_add_to_db',0)
    
    RUNTIME=wikicount.fnEndTimerCalcRuntime(STARTTIME)
    syslog.syslog('p3_add: EN records added in '+str(RUNTIME)+' seconds. Russian next.')
    LANGLIST=wikicount.getLanguageList()
    for lang in LANGLIST:
 	    ruFILE1="/tmp/"+str(lang)+"_action/q1_pagecounts.*"
	    ruFILE2="/tmp/"+str(lang)+"_action/q2_pagecounts.*"
	    ruFILE3="/tmp/"+str(lang)+"_action/q3_pagecounts.*"
	    ruFILE4="/tmp/"+str(lang)+"_action/q4_pagecounts.*"

	    STARTTIME=wikicount.fnStartTimer()
	    t = Process(target=UpdateHits, args=(ruFILE1,HOUR,DAY,MONTH,YEAR,lang))
	    u = Process(target=UpdateHits, args=(ruFILE2,HOUR,DAY,MONTH,YEAR,lang))
	    v = Process(target=UpdateHits, args=(ruFILE3,HOUR,DAY,MONTH,YEAR,lang))
	    w = Process(target=UpdateHits, args=(ruFILE4,HOUR,DAY,MONTH,YEAR,lang))
	
	    t.daemon=True
	    u.daemon=True
	    v.daemon=True
	    w.daemon=True

	    t.start()
	    time.sleep(1)
	    u.start()
	    time.sleep(1)
	    v.start()
	    time.sleep(1)
	    w.start()

	    t.join()
	    u.join()
	    v.join()
	    w.join()
	    RUNTIME=wikicount.fnEndTimerCalcRuntime(STARTTIME)
	    syslog.syslog('p3_add: Lang: '+str(lang)+' records added in '+str(RUNTIME)+' seconds.P3 Done now!')

    wikicount.fnSetStatusMsg('p3_add_to_db',3)
    wikicount.fnLaunchNextJob('p3_add_to_db')

