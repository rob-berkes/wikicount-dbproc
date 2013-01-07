import syslog
from pymongo import Connection 
conn=Connection()
db=conn.wc
syslog.syslog("starting")
rec=1
for result in db.map.find():
	if db.hits.find({'_id':result['_id']}):
		pass
	else:
		db.map.remove({'_id':result['_id']})
	rec+=1
