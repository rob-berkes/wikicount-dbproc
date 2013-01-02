#/usr/bin/python
from pymongo import Connection
from datetime import date
TODAY=date.today()
DAY=TODAY.day
MONTH=TODAY.month
YEAR=TODAY.year


conn=Connection()
db=conn.wc

FINDQUERY={'d':int(DAY),'m':int(MONTH),'y':int(YEAR)}
#db.prodtop.remove(FINDQUERY)
db.prodtop.remove()
TOPQUERY=db.tophits.find(FINDQUERY).sort('place',1).limit(50)

for line in TOPQUERY:
	POSTQUERY={'d':line['d'],'m':line['m'],'y':line['y'],'place':line['place'],'Hits':line['Hits'],'id':line['id']}
	db.prodtop.insert(POSTQUERY)


