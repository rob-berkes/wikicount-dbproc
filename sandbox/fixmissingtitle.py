#!/usr/bin/python
from pymongo import Connection

conn=Connection()
db=conn.wc

MISSINGT=db.tophits.find({'place':{'$lt':100},'title' :{'$exists':False}})
MISSINGTCOUNT=db.tophits.find({'title':{'$exists':False}}).count()
print str(MISSINGTCOUNT)+" records to process"
for item in MISSINGT:
	HITLIST=db.hits.find_one({'_id':item['id']})
	try:
		db.tophits.update({'d':item['d'],'m':item['m'],'y':item['y'],'id':item['id']},{'$set':{'title':HITLIST['title']}})
	except KeyError:
		print HITLIST
		pass 
	except TypeError:
		print HITLIST
		pass


