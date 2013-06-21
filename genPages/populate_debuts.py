from pymongo import Connection
from multiprocessing import Process
from functions import wikicount
import urllib2
RECORDSPERPAGE=100

def debuts_is_found(item):
    yy,ym,yd=wikicount.PreviousDay(YEAR,MONTH,DAY)
    YESTCOLL='tophits'+str(yy)+'_'+str(ym)+'_'+str(yd)
    if db[YESTCOLL].findOne({'_id':item['_id']}):
        return True
    else:
        return False

def debuts(RESULTSET,DAYKEY):
    dbCN='proddebuts'+DAYKEY
    debutCount=0
    print 'entering debut process'
    for item in RESULTSET:
        if not debuts_is_found(item) and debutCount<25:
            title, utitle=wikicount.FormatName(item['title'])
            try:
                POSTQ={'place':item['place'],'Hits':item['Hits'],'title':title,'linktitle':title,'id':item['id']}
                db[dbCN].insert(POSTQ)
                debutCount+=1
            except TypeError:
                pass
    return
STARTTIME=wikicount.fnStartTimer()
wikicount.toSyslog('populate_debuts.py:  starting...')
DAY,MONTH,YEAR,HOUR,expiretime=wikicount.fnReturnTimes()
HOUR=wikicount.minusHour(int(HOUR))
MONTHNAME=wikicount.fnGetMonthName()
DAYKEY=str(YEAR)+'_'+str(MONTH)+'_'+str(DAY)
COLLECTIONNAME='tophits'+str(DAYKEY)
wikicount.fnSetStatusMsg('populate_debuts',0)

conn=Connection()
db=conn.wc
RECCOUNT=1
NUMRECS=125
debutCount=0

dbCN='proddebuts'+COLLECTIONNAME
db[dbCN].remove()

RESULT1=db[COLLECTIONNAME].find().limit(NUMRECS).skip(0)
RESULT2=db[COLLECTIONNAME].find().limit(NUMRECS).skip(NUMRECS)
RESULT3=db[COLLECTIONNAME].find().limit(NUMRECS).skip(NUMRECS*2)
RESULT4=db[COLLECTIONNAME].find().limit(NUMRECS).skip(NUMRECS*3)
RESULT5=db[COLLECTIONNAME].find().limit(NUMRECS).skip(NUMRECS*4)
RESULT6=db[COLLECTIONNAME].find().limit(NUMRECS).skip(NUMRECS*5)
RESULT7=db[COLLECTIONNAME].find().limit(NUMRECS).skip(NUMRECS*6)
RESULT8=db[COLLECTIONNAME].find().limit(NUMRECS).skip(NUMRECS*7)

p = Process(target=debuts, args=(RESULT1,DAYKEY))
q = Process(target=debuts, args=(RESULT2,DAYKEY))
r = Process(target=debuts, args=(RESULT3,DAYKEY))
s = Process(target=debuts, args=(RESULT4,DAYKEY))
t = Process(target=debuts, args=(RESULT5,DAYKEY))
u = Process(target=debuts, args=(RESULT6,DAYKEY))
v = Process(target=debuts, args=(RESULT7,DAYKEY))
x = Process(target=debuts, args=(RESULT8,DAYKEY))

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
wikicount.toSyslog('populate_debuts: runtime is '+str(RUNTIME)+' seconds.')
wikicount.fnSetStatusMsg('populate_debuts',3)
wikicount.fnLaunchNextJob('populate_debuts')
