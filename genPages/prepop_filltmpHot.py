from pymongo import Connection
from datetime import date
from multiprocessing import Process
from functions import wikicount
import syslog
import string
import urllib2
RECORDSPERPAGE=100

def tmpHot(RESULTSET):
    yy,ym,yd=wikicount.PreviousDay(YEAR,MONTH,DAY)
    YESTCOLL='tophits'+str(yy)+'_'+str(ym)+'_'+str(yd)
    syslog.syslog("prepop-filltmpHot - thCN equals:"+str(thCN))
    YHits=0
    for item in RESULTSET:
        YHITS=db[YESTCOLL].find({'id':str(item['id'])})
        for ROW in YHITS:
            YHits+=ROW['Hits']
        delta=item['Hits']-YHits
        nameq=db.hitsdaily.find({'_id':item['_id']})
        NEWPOST={'id':item['_id'],'delta':int(delta),'orPlace':item['place'],'title':nameq['title']}
        db.tmpHot.insert(NEWPOST)

	
        return
STARTTIME=wikicount.fnStartTimer()
syslog.syslog('filltmpHot.py : starting...')
DAY,MONTH,YEAR,HOUR,expiretime=wikicount.fnReturnTimes()
HOUR=wikicount.minusHour(int(HOUR))
DAYKEY=str(YEAR)+'_'+str(MONTH)+'_'+str(DAY)
COLLECTIONNAME=str('tophits')+DAYKEY



conn=Connection()
db=conn.wc
RECCOUNT=1
NUMRECS=125
debutCount=0

wikicount.fnSetStatusMsg('fillTmpHot',0)

db.tmpHot.remove()
RESULT1=db[COLLECTIONNAME].find().limit(NUMRECS).skip(0)
RESULT2=db[COLLECTIONNAME].find().limit(NUMRECS).skip(NUMRECS)
RESULT3=db[COLLECTIONNAME].find().limit(NUMRECS).skip(NUMRECS*2)
RESULT4=db[COLLECTIONNAME].find().limit(NUMRECS).skip(NUMRECS*3)
RESULT5=db[COLLECTIONNAME].find().limit(NUMRECS).skip(NUMRECS*4)
RESULT6=db[COLLECTIONNAME].find().limit(NUMRECS).skip(NUMRECS*5)
RESULT7=db[COLLECTIONNAME].find().limit(NUMRECS).skip(NUMRECS*6)
RESULT8=db[COLLECTIONNAME].find().limit(NUMRECS).skip(NUMRECS*7)

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
