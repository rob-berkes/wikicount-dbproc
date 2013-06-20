#/usr/bin/python
from pymongo import Connection
from datetime import date
from functions import wikicount
import syslog
import time 

STARTTIME=wikicount.fnStartTimer()
syslog.syslog('tophits.py:  starting...')
DAY,MONTH,YEAR,HOUR,expiretime=wikicount.fnReturnTimes()
DAY,MONTH,HOUR=wikicount.fnFormatTimes(DAY,MONTH,HOUR)
MONTHNAME=wikicount.fnGetMonthName()
HOUR=wikicount.minusHour(int(HOUR))
conn=Connection()
db=conn.wc
RECCOUNT=1
DAYKEY=str(YEAR)+"_"+str(MONTH)+"_"+str(DAY)
COLLECTIONNAME="tophits"+str(DAYKEY)

db[COLLECTIONNAME].remove()

wikicount.fnSetStatusMsg('tophits',0)
IFILE=open("/home/ec2-user/mongo.csv.sorted","r")
RESULT=[]
for line in IFILE:
	if RECCOUNT < 250001:
		line=line.strip().split(",")
		RESULT.append((line[0],line[1]))
		RECCOUNT+=1
        Q={'_id':line[1]}
        TREC=db.hitsdaily.find_one(Q)
        title=TREC['title']
        INSERTREC={'_id':str(line[1]),'place':RECCOUNT,'Hits':int(line[0])}
        db[COLLECTIONNAME].insert(INSERTREC,safe=True)
	else:
		break
IFILE.close()

RUNTIME=wikicount.fnEndTimerCalcRuntime(STARTTIME)
syslog.syslog('tophits.py: runtime is '+str(RUNTIME)+' seconds.')
wikicount.fnSetStatusMsg('tophits',3)
wikicount.fnLaunchNextJob('tophits')
