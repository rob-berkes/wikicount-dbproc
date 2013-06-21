from pymongo import Connection
from multiprocessing import Process
from functions import wikicount
import syslog
import urllib2

def FillTmpHot(RESULTSET):
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
wikicount.toSyslog('filltmpHot.py : starting...')
DAY,MONTH,YEAR,HOUR,expiretime=wikicount.fnReturnTimes()
DAYKEY=str(YEAR)+'_'+str(MONTH)+'_'+str(DAY)
COLLECTIONNAME=str('tophits')+DAYKEY
print COLLECTIONNAME
conn=Connection()
db=conn.wc
RECCOUNT=1
NUMRECS=250
wikicount.fnSetStatusMsg('fillTmpHot',0)

db.tmpHot.remove()
RESULT1=db[COLLECTIONNAME].find().limit(NUMRECS).skip(0)
RESULT2=db[COLLECTIONNAME].find().limit(NUMRECS).skip(NUMRECS)
RESULT3=db[COLLECTIONNAME].find().limit(NUMRECS).skip(NUMRECS*2)
RESULT4=db[COLLECTIONNAME].find().limit(NUMRECS).skip(NUMRECS*3)

p = Process(target=FillTmpHot, args=RESULT1)
q = Process(target=FillTmpHot, args=RESULT2)
r = Process(target=FillTmpHot, args=RESULT3)
s = Process(target=FillTmpHot, args=RESULT4)

p.start()
q.start()
r.start()
s.start()

p.join()
q.join()
r.join()
s.join()
RUNTIME=wikicount.fnEndTimerCalcRuntime(STARTTIME)
wikicount.toSyslog('prepop_filltmpHot.py:  runtime is '+str(RUNTIME)+' seconds.')
wikicount.fnSetStatusMsg('fillTmpHot',3)
wikicount.fnLaunchNextJob('fillTmpHot')