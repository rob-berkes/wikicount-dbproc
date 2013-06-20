from pymongo import Connection
from multiprocessing import Process
from functions import wikicount
import urllib2
RECORDSPERPAGE=100

def debuts_is_found(item):

    return False

def debuts(RESULTSET,d,m,COLLECTIONNAME,NUMRECS,SKIPNUM):
    dbCN='proddebuts'+COLLECTIONNAME
    debutCount=0
    print 'entering debut process'
        for item in RESULTSET:
             if debuts_is_found(item) and debutCount<25:
                     title, utitle=wikicount.FormatName(item['title'])
                     try:
                        POSTQ={'d':d,'m':m,'y':y,'place':item['place'],'Hits':item['Hits'],'title':title,'linktitle':title,'id':item['id']}
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
COLLECTIONNAME=str(YEAR)+'_'+str(MONTH)+'_'+str(DAY)
wikicount.fnSetStatusMsg('populate_debuts',0)
yd,ym,y=wikicount.returnPrevDay(DAY,MONTH,YEAR)

conn=Connection()
db=conn.wc
RECCOUNT=1
NUMRECS=125
debutCount=0

dbCN='proddebuts'+COLLECTIONNAME
db[dbCN].remove({'d':DAY,'m': MONTH,'y':YEAR})

RESULT1=db['tophits'+COLLECTIONNAME].find().limit(NUMRECS).skip(0)
RESULT2=db['tophits'+COLLECTIONNAME].find().limit(NUMRECS).skip(NUMRECS)
RESULT3=db['tophits'+COLLECTIONNAME].find().limit(NUMRECS).skip(NUMRECS*2)
RESULT4=db['tophits'+COLLECTIONNAME].find().limit(NUMRECS).skip(NUMRECS*3)
RESULT5=db['tophits'+COLLECTIONNAME].find().limit(NUMRECS).skip(NUMRECS*4)
RESULT6=db['tophits'+COLLECTIONNAME].find().limit(NUMRECS).skip(NUMRECS*5)
RESULT7=db['tophits'+COLLECTIONNAME].find().limit(NUMRECS).skip(NUMRECS*6)
RESULT8=db['tophits'+COLLECTIONNAME].find().limit(NUMRECS).skip(NUMRECS*7)

p = Process(target=debuts, args=(RESULT1,d,m,COLLECTIONNAME,NUMRECS,0))
q = Process(target=debuts, args=(RESULT2,d,m,COLLECTIONNAME,NUMRECS,1))
r = Process(target=debuts, args=(RESULT3,d,m,COLLECTIONNAME,NUMRECS,2))
s = Process(target=debuts, args=(RESULT4,d,m,COLLECTIONNAME,NUMRECS,3))
t = Process(target=debuts, args=(RESULT5,d,m,COLLECTIONNAME,NUMRECS,4))
u = Process(target=debuts, args=(RESULT6,d,m,COLLECTIONNAME,NUMRECS,5))
v = Process(target=debuts, args=(RESULT7,d,m,COLLECTIONNAME,NUMRECS,6))
x = Process(target=debuts, args=(RESULT8,d,m,COLLECTIONNAME,NUMRECS,7))

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
