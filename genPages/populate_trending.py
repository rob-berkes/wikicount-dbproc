from pymongo import Connection
from datetime import date
from functions import wikicount
import syslog
import string
import urllib2
RECORDSPERPAGE=100

def cold(d,m):
	TRENDING_LIST_QUERY=db.tmpHot.find().sort('delta',-1).limit(100)
        title=''
        for p in TRENDING_LIST_QUERY:
                title,utitle=wikicount.FormatName(p['title'])
                rec={'title':title,'place':p['orPlace'],'Hits':p['delta'],'linktitle':utitle,'d':d,'m':m,'y':y,'id':p['id']}
                db.prodtrend.insert(rec)

        return

STARTTIME=wikicount.fnStartTimer()
syslog.syslog('populate_trending.py : starting...')
DAY,MONTH,YEAR,HOUR,expiretime=wikicount.fnReturnTimes()
HOUR=wikicount.minusHour(int(HOUR))
MONTHNAME=wikicount.fnGetMonthName()
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
wikicount.fnSetStatusMsg('populate_trending',0)

db.prodtrend.remove()
cold(d,m)

RUNTIME=wikicount.fnEndTimerCalcRuntime(STARTTIME)
syslog.syslog('populate_trending.py: runtime is '+str(RUNTIME)+' seconds.') 
wikicount.fnSetStatusMsg('populate_trending',3)
wikicount.fnLaunchNextJob('populate_trending')
