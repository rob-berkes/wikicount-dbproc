#/usr/bin/python
from pymongo import Connection

conn=Connection()

db=conn.wc
OFILE=open("removable.ids","w")
#QUERY=db.map.find()
QUERY=db.map.find({"title":{"$regex":'.php'}})
count=0
totalcount=0
for line in QUERY:
	hitcount=0
#	HQUERY=db.hits.find({'_id':line['_id']})
#	for line in HQUERY:
#		hitcount=1
#	totalcount+=1
#	if hitcount==0 :
	OFILE.write(line['_id']+'\n')
	db.hits.remove({'_id':line['_id']})
	db.map.remove({'_id':line['_id']})
#		db.map.remove({'_id':line['_id']})
	count+=1	

print count,'/',totalcount
OFILE.close()
