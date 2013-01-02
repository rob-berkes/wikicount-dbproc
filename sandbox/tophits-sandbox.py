#/usr/bin/python
from pymongo import Connection
from datetime import date
from multiprocessing import Process

ddate=date.today()
d=ddate.day
m=ddate.month
y=ddate.year
conn=Connection()
db=conn.wc
db.tmpHitReport.drop()
RECCOUNT=1
RESULT=db.hits.find().sort('Hits',-1).limit(500000)
#def f(RESULT):
for item in RESULT:
	Hits=int(item['Hits'])
	yd=int(d)-1
	YHITS=db.tophits.find({'d':yd,'id':str(item['_id'])})
	for row in YHITS:
		Hits=Hits-row['Hits']
	NEWPOST={'id':item['_id'],'Hits':Hits}
	db.tmpHitReport.insert(NEWPOST)
        RECCOUNT+=1



#NEWRESULT=db.tmpHitReport.find().sort('Hits',-1).limit(100)

#RECC=0
#for item in NEWRESULT:
#	RECC+=1
#	NEWPOST={'id':item['id'],'place':int(RECC),'Hits':item['Hits'],'d':d,'m':m,'y':y}
#	NAME='<null>'
#	NQUERY={'_id':str(item['id'])}
#	RES=db.map.find(NQUERY)
#	for i in RES:
#		NAME=i['title']
#	print RECC,NAME,item['Hits']
#	db.tmpTopHits.insert(NEWPOST)
##db.tmpHitReport.drop()
##RESULT.rewind()
##for item2 in RESULT:
##	print RECC,item2
##	RECC+=1
##print RESULT
