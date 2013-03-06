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
COLLECTIONNAME="tophits"+str(YEAR)+str(MONTHNAME)
conn=Connection()
db=conn.wc
RECCOUNT=1
DAYKEY=str(YEAR)+"_"+str(MONTH)+"_"+str(DAY)


wikicount.fnSetStatusMsg('tophits',0)
IFILE=open("/home/ec2-user/mongo.csv.sorted","r")
RESULT=[]
for line in IFILE:
	if RECCOUNT < 250001:
		line=line.strip().split(",")
		RESULT.append((line[0],line[1]))
		RECCOUNT+=1
	else:
		break
IFILE.close()
#RESULT=db.hitsdaily.find({DAYKEY:{'$exists':True}}).sort(DAYKEY,-1).limit(250000)

db[COLLECTIONNAME].remove({'d':int(DAY),'m':int(MONTH),'y':int(YEAR)})
RECCOUNT=1
for item in RESULT:
	Q={'_id':item[1]}
	TREC=db.hitsdaily.find(Q)
	title=''
	try:
		for a in TREC:
			title=a['title']
			INSERTREC={'id':str(item[1]),'d':int(DAY),'m':int(MONTH),'y':int(YEAR),'place':RECCOUNT,'Hits':int(item[0]),'title':title}
			insert=db[COLLECTIONNAME].insert(INSERTREC,safe=True)
			insert
	except KeyError:
		pass
	RECCOUNT+=1
	if RECCOUNT > 250000:
		break

RUNTIME=wikicount.fnEndTimerCalcRuntime(STARTTIME)
syslog.syslog('tophits.py: runtime is '+str(RUNTIME)+' seconds.')
wikicount.fnSetStatusMsg('tophits',3)
wikicount.fnLaunchNextJob('tophits')
