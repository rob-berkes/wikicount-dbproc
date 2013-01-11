#/usr/bin/python
from pymongo import Connection
from datetime import date
from functions import wikicount
DAY,MONTH,YEAR,HOUR,expiretime=wikicount.fnReturnTimes()
conn=Connection()
db=conn.wc
RECCOUNT=1
RESULT=db.hits.find().sort('Hits',-1).limit(250000)

db.tophits.remove({'d':DAY,'m':MONTH,'y':YEAR})
for item in RESULT:
        db.tophits.insert({'id':str(item['_id']),'d':DAY,'m':MONTH,'y':YEAR,'place':RECCOUNT,'Hits':int(item['Hits']),'title':item['title']},safe=True)
	key=str(YEAR)+'_'+str(MONTH)+'_'+str(DAY)
        db.dailytop.update({'_id':str(item['_id'])},
                           {
                                '$set':{key:{'Hits':int(item['Hits']),
                                'Place':RECCOUNT}}
                           },upsert=True)

        RECCOUNT+=1

