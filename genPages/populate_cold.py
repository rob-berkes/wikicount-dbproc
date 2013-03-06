from pymongo import Connection
from datetime import date
from multiprocessing import Process
from functions import wikicount
import syslog
import string
import urllib2
RECORDSPERPAGE=100

def cold(d,m):
  	COLD_LIST_QUERY=db.tmpHot.find().sort('delta',1).limit(100)
        title=''
        for p in COLD_LIST_QUERY:
                title,utitle=wikicount.FormatName(p['title'])
                rec={'title':utitle,'place':p['orPlace'],'Hits':p['delta'],'linktitle':title,'d':d,'m':m,'y':y,'id':p['id']}
                db.prodcold.insert(rec)
	
        return
STARTTIME=wikicount.fnStartTimer()
syslog.syslog('populate_cold:  starting...')
DAY,MONTH,YEAR,HOUR,expiretime=wikicount.fnReturnTimes()
HOUR=wikicount.minusHour(int(HOUR))
MONTHNAME=wikicount.fnGetMonthName()
COLLECTIONNAME=str(YEAR)+MONTHNAME
d=DAY
yd=int(DAY)-1
if yd==0:
        yd=30
m=MONTH
ym=MONTH
if yd==30:
        ym=int(m)-1
y=YEAR

conn=Connection()
db=conn.wc
RECCOUNT=1
NUMRECS=62500
debutCount=0
wikicount.fnSetStatusMsg('populate_cold',0)
db.prodcold.remove()

p = Process(target=cold, args=(d,m))

p.start()

p.join()

RUNTIME=wikicount.fnEndTimerCalcRuntime(STARTTIME)
syslog.syslog('populate_cold: runtime is '+str(RUNTIME)+' seconds.')
wikicount.fnSetStatusMsg('populate_cold',3)
wikicount.fnLaunchNextJob('populate_cold')
