#/usr/bin/python
from pymongo import Connection
from datetime import date
import string
import urllib2
import functions




conn=Connection()

db=conn.wc

TODAY=date.today()
y=TODAY.year
m=TODAY.month
d=TODAY.day

yy=y
ym=m
yd=d-1

if yd==0:
	yd=30
	ym=m-1
	if ym==0:
		ym=12
		yy-=1

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
             TRESULT=db.map.find_one({'_id':item['id']})
	     utitle=urllib2.unquote(item['id'])	
	     POSTQ={'d':d,'m':m,'y':y,'place':item['place'],'title':TRESULT['title'],'Hits':item['Hits'],'title':utitle,'linktitle':TRESULT['title'].encode('utf-8'),'id':item['id']}
	     db.proddebuts.insert(POSTQ)	     

