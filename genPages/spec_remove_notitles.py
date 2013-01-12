from pymongo import Connection
conn=Connection()
db=conn.wc

for RES in db.hits.find():
	try:
		if RES['title']:
			pass
	except KeyError:
		db.hits.remove({'_id':RES['_id']})
