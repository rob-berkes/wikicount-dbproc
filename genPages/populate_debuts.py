from pymongo import Connection
from datetime import date
from multiprocessing import Process
from functions import wikicount

import string
import urllib2
RECORDSPERPAGE=100

def debuts(RESULTSET,d,m,COLLECTIONNAME,NUMRECS,SKIPNUM):
	thCN='tophits'+COLLECTIONNAME
        dbCN='proddebuts'+COLLECTIONNAME
	debutCount=0
	print 'entering debut process'
        for item in RESULTSET:
             YQUERY={'id':item['id']}
             if db[thCN].find(YQUERY).count() == 1 and debutCount<25:
                     title, utitle=wikicount.FormatName(item['title'])
                     try:
                        POSTQ={'d':d,'m':m,'y':y,'place':item['place'],'Hits':item['Hits'],'title':title,'linktitle':title,'id':item['id']}
                        db[dbCN].insert(POSTQ)
                        debutCount+=1
                     except TypeError:
                        pass

        return
DAY,MONTH,YEAR,HOUR,expiretime=wikicount.fnReturnTimes()
HOUR=wikicount.minusHour(int(HOUR))
MONTHNAME=wikicount.fnGetMonthName()
COLLECTIONNAME=str(YEAR)+MONTHNAME
wikicount.fnSetStatusMsg('populate_debuts',0)
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

dbCN='proddebuts'+COLLECTIONNAME
db[dbCN].remove({'d':d,'m':m,'y':y})

RESULT1=db['tophits'+COLLECTIONNAME].find({'d':d,'m':m,'y':y}).limit(NUMRECS).skip(0)
RESULT2=db['tophits'+COLLECTIONNAME].find({'d':d,'m':m,'y':y}).limit(NUMRECS).skip(NUMRECS)
RESULT3=db['tophits'+COLLECTIONNAME].find({'d':d,'m':m,'y':y}).limit(NUMRECS).skip(NUMRECS*2)
RESULT4=db['tophits'+COLLECTIONNAME].find({'d':d,'m':m,'y':y}).limit(NUMRECS).skip(NUMRECS*3)
RESULT5=db['tophits'+COLLECTIONNAME].find({'d':d,'m':m,'y':y}).limit(NUMRECS).skip(NUMRECS*4)
RESULT6=db['tophits'+COLLECTIONNAME].find({'d':d,'m':m,'y':y}).limit(NUMRECS).skip(NUMRECS*5)
RESULT7=db['tophits'+COLLECTIONNAME].find({'d':d,'m':m,'y':y}).limit(NUMRECS).skip(NUMRECS*6)
RESULT8=db['tophits'+COLLECTIONNAME].find({'d':d,'m':m,'y':y}).limit(NUMRECS).skip(NUMRECS*7)

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
wikicount.fnSetStatusMsg('populate_debuts',1)
