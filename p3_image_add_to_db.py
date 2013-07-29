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
syslog.syslog('p3_image_add: starting....')
DAY,MONTH,YEAR,HOUR,expiretime=wikicount.fnReturnTimes()
HOUR=wikicount.minusHour(int(HOUR))
DAY,MONTH,HOUR=wikicount.fnFormatTimes(DAY,MONTH,HOUR)

def UpdateHits(FILEPROC,HOUR,DAY,MONTH,YEAR,LANG):
     IMAGECNAME=str(LANG)+"_image"
     IMAGECHNAME=str(LANG)+"_imagehourly"
     IMAGECDNAME=str(LANG)+"_imagedaily"
     connection=Connection()
     db=connection.wc
     RECORDS=0
     DAYKEY=str(YEAR)+"_"+str(MONTH)+"_"+str(DAY)
     print "images "+str(DAYKEY)
     for FILENAME in glob.glob(FILEPROC):	
	try:
		IFILE2=open(FILENAME,'r')
		for line in IFILE2:
			try:
		          line=line.strip().split()
		          HASH=hashlib.sha1(line[1]).hexdigest()
		          POSTFIND={'_id': HASH}
			  TITLESTRING=line[1].decode('utf-8')
		          db[IMAGECNAME].update(POSTFIND,{ "$inc" : { "Hits" : int(line[2]) },"$set":{'title':TITLESTRING} },upsert=True)
			  db[IMAGECHNAME].update(POSTFIND,{"$inc":{HOUR:int(line[2])}},upsert=True)
			  db[IMAGECDNAME].update(POSTFIND,{"$inc":{DAYKEY: int(line[2])}},upsert=True)
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







if __name__ == '__main__':
    wikicount.fnSetStatusMsg('p3_image_add',0)
    LANGUAGES=wikicount.getLanguageList()
    for lang in LANGUAGES:
	    FILEPROC2="/tmp/"+str(lang)+"_image/q2_pagecounts.*"
	    FILEPROC3="/tmp/"+str(lang)+"_image/q3_pagecounts.*"
	    FILEPROC4="/tmp/"+str(lang)+"_image/q4_pagecounts.*"
	    FILEPROC5="/tmp/"+str(lang)+"_image/q1_pagecounts.*"
	    p = Process(target=UpdateHits, args=(FILEPROC2,HOUR,DAY,MONTH,YEAR,str(lang)))
	    q = Process(target=UpdateHits, args=(FILEPROC3,HOUR,DAY,MONTH,YEAR,str(lang)))
	    r = Process(target=UpdateHits, args=(FILEPROC4,HOUR,DAY,MONTH,YEAR,str(lang)))
	    s = Process(target=UpdateHits, args=(FILEPROC5,HOUR,DAY,MONTH,YEAR,str(lang)))
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
	    syslog.syslog("p3_image_add: runtime "+str(RUNTIME)+' seconds')
    wikicount.fnSetStatusMsg('p3_image_add',3)
#    wikicount.fnLaunchNextJob('p3_image_add')
