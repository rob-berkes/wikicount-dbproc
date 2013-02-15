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
	for item in RESULTSET:
		Hits=int(item['Hits'])
		YHITS=db[thCN].find({'d':yd,'m':ym,'y':y,'id':str(item['id'])})
		for ROW in YHITS:
			Hits=Hits-ROW['Hits']
		NEWPOST={'id':item['id'],'delta':Hits,'orPlace':item['place'],'title':item['title']}
		OUTPUT.append(NEWPOST)
	for POSTQ in OUTPUT:
		db.tmpHot.insert(POSTQ)


	TODAY=date.today()
        DAY=TODAY.day
        MONTH=TODAY.month
        YEAR=TODAY.year
#       FQUERY={'d':int(DAY),'m':int(MONTH),'y':int(YEAR)}
        TRENDING_LIST_QUERY=db.tmpHot.find().sort('delta',-1).limit(NUMRECS).skip(NUMRECS*SKIPNUM)
        send_list=[]
        title=''
        for p in TRENDING_LIST_QUERY:
		title,utitle=wikicount.FormatName(p['title'])
                rec={'title':title,'place':p['orPlace'],'Hits':p['delta'],'linktitle':utitle,'d':yd,'m':ym,'y':y,'id':p['id']}
		db.prodtrend.insert(rec) 
        COLD_LIST_QUERY=db.tmpHot.find().sort('delta',1).limit(100)
        send_list=[]
        title=''
        for p in COLD_LIST_QUERY:
		title,utitle=wikicount.FormatName(p['title'])
                rec={'title':utitle,'place':p['orPlace'],'Hits':p['delta'],'linktitle':title,'d':yd,'m':ym,'y':y,'id':p['id']}
		db.prodcold.insert(rec)
	


	RESULTSET.rewind()
	debutCount=1
	print 'entering debut process'
	for item in RESULTSET:
	     YQUERY={'id':item['id']}
	     if db[thCN].find(YQUERY).count() == 1 and debutCount<150:
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
db.prodtrend.drop()
db.prodcold.remove()
REMOVEQ={'d':d,'m':m,'y':y}
db.proddebuts.remove(REMOVEQ)
INSERTSET=[]
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
