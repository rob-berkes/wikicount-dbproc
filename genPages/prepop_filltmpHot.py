from pymongo import Connection
from datetime import date
from multiprocessing import Process
from functions import wikicount
import syslog
import string
import urllib2
RECORDSPERPAGE=100

def tmpHot(RESULTSET,d,m,COLLECTIONNAME,NUMRECS,SKIPNUM):
	OUTPUT=[]
        thCN='tophits'+COLLECTIONNAME
        dbCN='proddebuts'+COLLECTIONNAME
        for item in RESULTSET:
                Hits=int(item['Hits'])
                YHITS=db[thCN].find({'id':str(item['id'])})
                for ROW in YHITS:
                        Hits=Hits-ROW['Hits']
                NEWPOST={'id':item['id'],'delta':Hits,'orPlace':item['place'],'title':item['title']}
                db.tmpHot.insert(NEWPOST)

	
        return
STARTTIME=wikicount.fnStartTimer()
syslog.syslog('filltmpHot.py : starting...')
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

wikicount.fnSetStatusMsg('fillTmpHot',0)

db.tmpHot.remove()
RESULT1=db['tophits'+COLLECTIONNAME].find({'d':d,'m':m,'y':y}).limit(NUMRECS).skip(0)
RESULT2=db['tophits'+COLLECTIONNAME].find({'d':d,'m':m,'y':y}).limit(NUMRECS).skip(NUMRECS)
RESULT3=db['tophits'+COLLECTIONNAME].find({'d':d,'m':m,'y':y}).limit(NUMRECS).skip(NUMRECS*2)
RESULT4=db['tophits'+COLLECTIONNAME].find({'d':d,'m':m,'y':y}).limit(NUMRECS).skip(NUMRECS*3)
RESULT5=db['tophits'+COLLECTIONNAME].find({'d':d,'m':m,'y':y}).limit(NUMRECS).skip(NUMRECS*4)
RESULT6=db['tophits'+COLLECTIONNAME].find({'d':d,'m':m,'y':y}).limit(NUMRECS).skip(NUMRECS*5)
RESULT7=db['tophits'+COLLECTIONNAME].find({'d':d,'m':m,'y':y}).limit(NUMRECS).skip(NUMRECS*6)
RESULT8=db['tophits'+COLLECTIONNAME].find({'d':d,'m':m,'y':y}).limit(NUMRECS).skip(NUMRECS*7)

p = Process(target=tmpHot, args=(RESULT1,d,m,COLLECTIONNAME,NUMRECS,0))
q = Process(target=tmpHot, args=(RESULT2,d,m,COLLECTIONNAME,NUMRECS,1))
r = Process(target=tmpHot, args=(RESULT3,d,m,COLLECTIONNAME,NUMRECS,2))
s = Process(target=tmpHot, args=(RESULT4,d,m,COLLECTIONNAME,NUMRECS,3))
t = Process(target=tmpHot, args=(RESULT5,d,m,COLLECTIONNAME,NUMRECS,4))
u = Process(target=tmpHot, args=(RESULT6,d,m,COLLECTIONNAME,NUMRECS,5))
v = Process(target=tmpHot, args=(RESULT7,d,m,COLLECTIONNAME,NUMRECS,6))
x = Process(target=tmpHot, args=(RESULT8,d,m,COLLECTIONNAME,NUMRECS,7))

p.start()
q.start()
r.start()
s.start()
t.start()
u.start()
v.start()
x.start()

p.join()
q.join()
r.join()
s.join()
t.join()
u.join()
v.join()
x.join() 
RUNTIME=wikicount.fnEndTimerCalcRuntime(STARTTIME)
syslog.syslog('prepop_filltmpHot.py:  runtime is '+str(RUNTIME)+' seconds.')
wikicount.fnSetStatusMsg('fillTmpHot',3)
wikicount.fnLaunchNextJob('fillTmpHot')
