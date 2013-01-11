#/usr/bin/python
from pymongo import Connection
from datetime import date
from multiprocessing import Process
from functions import wikicount

import string
import urllib2
RECORDSPERPAGE=100
def f(RESULTSET,yd,ym):
	OUTPUT=[]
	for item in RESULTSET:
		Hits=int(item['Hits'])
		YHITS=db.tophits.find({'d':yd,'m':ym,'y':y,'id':str(item['id'])})
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
        TRENDING_LIST_QUERY=db.tmpHot.find().sort('delta',-1).limit(100)
        send_list=[]
        title=''
	db.prodtrend.drop()
        for p in TRENDING_LIST_QUERY:
		title,utitle=wikicount.FormatName(p['title'])
                rec={'title':title,'place':p['orPlace'],'Hits':p['delta'],'linktitle':utitle,'d':yd,'m':ym,'y':y,'id':p['id']}
		db.prodtrend.insert(rec) 
        COLD_LIST_QUERY=db.tmpHot.find().sort('delta',1).limit(100)
        send_list=[]
        title=''
	db.prodcold.remove()
        for p in COLD_LIST_QUERY:
		title,utitle=wikicount.FormatName(p['title'])
                rec={'title':utitle,'place':p['orPlace'],'Hits':p['delta'],'linktitle':title,'d':yd,'m':ym,'y':y,'id':p['id']}
		db.prodcold.insert(rec)

	REMOVEQ={'d':d,'m':m,'y':y}
	db.proddebuts.remove(REMOVEQ)
	TODAYQUERY={'d':d,'m':m,'y':y,'place':{'$lt':100000}}
	CHECKLIST=db.tophits.find(TODAYQUERY)
	for item in CHECKLIST:
	     YQUERY={'d':yd,'m':m,'y':y,'id':item['id']}
	     YRESULT=db.tophits.find_one(YQUERY)
	     if YRESULT:
	             pass
	     else:
		     TRESULT={}
	             TRESULT=db.hits.find_one({'_id':item['id']})
		     title, utitle=wikicount.FormatName(TRESULT['title'])
		     try:
	             	POSTQ={'d':d,'m':m,'y':y,'place':item['place'],'title':TRESULT['title'],'Hits':item['Hits'],'title':utitle,'linktitle':TRESULT['title'].encode('utf-8'),'id':item['id']}
	             	db.proddebuts.insert(POSTQ)
		     except TypeError:
			pass

	return

DAY,MONTH,YEAR,HOUR,expiretime=wikicount.fnReturnTimes()
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
db.tmpHot.drop()
INSERTSET=[]
RESULT1=db.tophits.find({'d':d,'m':m,'y':y}).limit(NUMRECS).skip(0)
RESULT2=db.tophits.find({'d':d,'m':m,'y':y}).limit(NUMRECS).skip(NUMRECS)
RESULT3=db.tophits.find({'d':d,'m':m,'y':y}).limit(NUMRECS).skip(NUMRECS*2)
RESULT4=db.tophits.find({'d':d,'m':m,'y':y}).limit(NUMRECS).skip(NUMRECS*3)
RESULT5=db.tophits.find({'d':d,'m':m,'y':y}).limit(NUMRECS).skip(NUMRECS*4)
RESULT6=db.tophits.find({'d':d,'m':m,'y':y}).limit(NUMRECS).skip(NUMRECS*5)
RESULT7=db.tophits.find({'d':d,'m':m,'y':y}).limit(NUMRECS).skip(NUMRECS*6)
RESULT8=db.tophits.find({'d':d,'m':m,'y':y}).limit(NUMRECS).skip(NUMRECS*7)

p = Process(target=f, args=(RESULT1,yd,m))
q = Process(target=f, args=(RESULT2,yd,m))
r = Process(target=f, args=(RESULT3,yd,m))
s = Process(target=f, args=(RESULT4,yd,m))
t = Process(target=f, args=(RESULT5,yd,m))
u = Process(target=f, args=(RESULT6,yd,m))
v = Process(target=f, args=(RESULT7,yd,m))
x = Process(target=f, args=(RESULT8,yd,m))

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
