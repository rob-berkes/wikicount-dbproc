import syslog
from pymongo import Connection 
conn=Connection()
db=conn.wc

rec=1
for result in db.map.find():
	if db.hits.find({'_id':result['_id']}):
		pass
	else:
		syslog.syslog("Rec number: "+str(rec)+" "+str(result['title']))
	rec+=1
