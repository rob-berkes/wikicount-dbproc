#/usr/bin/python
from pymongo import Connection
from datetime import date
from multiprocessing import Process
from functions import wikicount

import string
import urllib2
RECORDSPERPAGE=100
def f(RESULTSET,yd,ym,COLLECTIONNAME,NUMRECS,SKIPNUM):
	OUTPUT=[]
	thCN='tophits'+COLLECTIONNAME
	dbCN='proddebuts'+COLLECTIONNAME


	TODAY=date.today()
        DAY=TODAY.day
        MONTH=TODAY.month
        YEAR=TODAY.year
#       FQUERY={'d':int(DAY),'m':int(MONTH),'y':int(YEAR)}
	

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
db.tmpHot.remove()
db['tmpHot'].create_index('delta')
db.prodtrend.remove()
db.prodcold.remove()
REMOVEQ={'d':d,'m':m,'y':y}
db.proddebuts.remove(REMOVEQ)
INSERTSET=[]
SYEAR='%02d' % (int(YEAR),)
SDAY='%02d' % (int(DAY),)
SMONTH='%02d' % (int(MONTH),)

DAYSTR=str(YEAR)+"_"+str(SMONTH)+"_"+str(SDAY)
CAT_TRENDING_LIST_QUERY=db.categorydaily.find().sort(DAYSTR,-1).limit(100)
send_list=[]
title=''
MTITLE=''
db.prodcattrend.drop()
for p in CAT_TRENDING_LIST_QUERY:
	MQUERY={"_id":str(p['_id'])}
	for row in db.category.find(MQUERY):
		MTITLE=str(row['title'])
	title,utitle=wikicount.FormatName(MTITLE)
        rec={'title':title,'place':p[DAYSTR],'Hits':p[DAYSTR],'linktitle':utitle,'d':yd,'m':ym,'y':y,'id':p['_id']}
	db.prodcattrend.insert(rec)

RESULT1=db['tophits'+COLLECTIONNAME].find({'d':d,'m':m,'y':y}).limit(NUMRECS).skip(0)
RESULT2=db['tophits'+COLLECTIONNAME].find({'d':d,'m':m,'y':y}).limit(NUMRECS).skip(NUMRECS)
RESULT3=db['tophits'+COLLECTIONNAME].find({'d':d,'m':m,'y':y}).limit(NUMRECS).skip(NUMRECS*2)
RESULT4=db['tophits'+COLLECTIONNAME].find({'d':d,'m':m,'y':y}).limit(NUMRECS).skip(NUMRECS*3)
RESULT5=db['tophits'+COLLECTIONNAME].find({'d':d,'m':m,'y':y}).limit(NUMRECS).skip(NUMRECS*4)
RESULT6=db['tophits'+COLLECTIONNAME].find({'d':d,'m':m,'y':y}).limit(NUMRECS).skip(NUMRECS*5)
RESULT7=db['tophits'+COLLECTIONNAME].find({'d':d,'m':m,'y':y}).limit(NUMRECS).skip(NUMRECS*6)
RESULT8=db['tophits'+COLLECTIONNAME].find({'d':d,'m':m,'y':y}).limit(NUMRECS).skip(NUMRECS*7)

p = Process(target=f, args=(RESULT1,yd,m,COLLECTIONNAME,NUMRECS,0))
q = Process(target=f, args=(RESULT2,yd,m,COLLECTIONNAME,NUMRECS,1))
r = Process(target=f, args=(RESULT3,yd,m,COLLECTIONNAME,NUMRECS,2))
s = Process(target=f, args=(RESULT4,yd,m,COLLECTIONNAME,NUMRECS,3))
t = Process(target=f, args=(RESULT5,yd,m,COLLECTIONNAME,NUMRECS,4))
u = Process(target=f, args=(RESULT6,yd,m,COLLECTIONNAME,NUMRECS,5))
v = Process(target=f, args=(RESULT7,yd,m,COLLECTIONNAME,NUMRECS,6))
x = Process(target=f, args=(RESULT8,yd,m,COLLECTIONNAME,NUMRECS,7))

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
t.join()
u.join()
v.join()
x.join()
