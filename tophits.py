#/usr/bin/python
from pymongo import Connection
from datetime import date

ddate=date.today()
d=ddate.day
m=ddate.month
y=ddate.year
conn=Connection()
db=conn.wc
RECCOUNT=1
RESULT=db.hits.find().sort('Hits',-1).limit(250000)

db.tophits.remove({'d':d,'m':m,'y':y})
for item in RESULT:
        db.tophits.insert({'id':str(item['_id']),'d':d,'m':m,'y':y,'place':RECCOUNT,'Hits':int(item['Hits'])},safe=True)
        RECCOUNT+=1

