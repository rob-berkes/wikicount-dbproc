from pymongo import Connection
from datetime import date
from multiprocessing import Process
from functions import wikicount

import string
import urllib2
RECORDSPERPAGE=100

def cold(RESULTSET,d,m,COLLECTIONNAME,NUMRECS,SKIPNUM):
	thCN='tophits'+COLLECTIONNAME
  	COLD_LIST_QUERY=db.tmpHot.find().sort('delta',1).limit(100)
        title=''
        for p in COLD_LIST_QUERY:
                title,utitle=wikicount.FormatName(p['title'])
                rec={'title':utitle,'place':p['orPlace'],'Hits':p['delta'],'linktitle':title,'d':yd,'m':ym,'y':y,'id':p['id']}
                db.prodcold.insert(rec)
	
        return
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
wikicount.fnSetStatusMsg('populate_cold',0)
db.prodcold.remove()
RESULT1=db['tophits'+COLLECTIONNAME].find({'d':d,'m':m,'y':y}).limit(NUMRECS).skip(0)
RESULT2=db['tophits'+COLLECTIONNAME].find({'d':d,'m':m,'y':y}).limit(NUMRECS).skip(NUMRECS)
RESULT3=db['tophits'+COLLECTIONNAME].find({'d':d,'m':m,'y':y}).limit(NUMRECS).skip(NUMRECS*2)
RESULT4=db['tophits'+COLLECTIONNAME].find({'d':d,'m':m,'y':y}).limit(NUMRECS).skip(NUMRECS*3)
RESULT5=db['tophits'+COLLECTIONNAME].find({'d':d,'m':m,'y':y}).limit(NUMRECS).skip(NUMRECS*4)
RESULT6=db['tophits'+COLLECTIONNAME].find({'d':d,'m':m,'y':y}).limit(NUMRECS).skip(NUMRECS*5)
RESULT7=db['tophits'+COLLECTIONNAME].find({'d':d,'m':m,'y':y}).limit(NUMRECS).skip(NUMRECS*6)
RESULT8=db['tophits'+COLLECTIONNAME].find({'d':d,'m':m,'y':y}).limit(NUMRECS).skip(NUMRECS*7)

p = Process(target=cold, args=(RESULT1,d,m,COLLECTIONNAME,NUMRECS,0))
q = Process(target=cold, args=(RESULT2,d,m,COLLECTIONNAME,NUMRECS,1))
r = Process(target=cold, args=(RESULT3,d,m,COLLECTIONNAME,NUMRECS,2))
s = Process(target=cold, args=(RESULT4,d,m,COLLECTIONNAME,NUMRECS,3))
t = Process(target=cold, args=(RESULT5,d,m,COLLECTIONNAME,NUMRECS,4))
u = Process(target=cold, args=(RESULT6,d,m,COLLECTIONNAME,NUMRECS,5))
v = Process(target=cold, args=(RESULT7,d,m,COLLECTIONNAME,NUMRECS,6))
x = Process(target=cold, args=(RESULT8,d,m,COLLECTIONNAME,NUMRECS,7))

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
wikicount.fnSetStatusMsg('populate_cold',1)
