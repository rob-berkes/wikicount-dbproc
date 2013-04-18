from pymongo import Connection
from datetime import date
from multiprocessing import Process
from functions import wikicount
import syslog
import string
import urllib2
RECORDSPERPAGE=100

def image(RESULTSET,d,m,COLLECTIONNAME,NUMRECS,SKIPNUM):
	
        return
STARTTIME=wikicount.fnStartTimer()
syslog.syslog('populate_image: starting...')
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
NUMRECS=31250
debutCount=0
wikicount.fnSetStatusMsg('populate_image',0)
db.prodimagetrend.remove()
RESULT1=[]

YEARSTR=wikicount.fnReturnTimeString(DAY,MONTH,YEAR)
CATEGORY_LIST_QUERY=db.imagedaily.find().sort(YEARSTR,-1).limit(100)
title=''
for p in CATEGORY_LIST_QUERY:
	title,utitle=wikicount.MapQuery_FindImageName(p['_id'])
	try:
	        rec={'title':utitle,'place':p[YEARSTR],'Hits':p[YEARSTR],'linktitle':title,'d':yd,'m':ym,'y':y,'id':p['_id']}
	        db.prodimagetrend.insert(rec)
	except KeyError:
		pass 
RUNTIME=wikicount.fnEndTimerCalcRuntime(STARTTIME)
syslog.syslog('populate_image:  runtime is '+str(RUNTIME)+' seconds.')
wikicount.fnSetStatusMsg('populate_image',3)
wikicount.fnLaunchNextJob('populate_image')
