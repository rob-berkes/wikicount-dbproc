from pymongo import Connection
conn=Connection()
db=conn.wc
a=0
b=0
k=0
for RES in db.hitshourly.find():
	try:
		if db.hits.find_one({'_id':RES['_id']}):
			pass
		else:
			db.hitshourly.remove({'_id':RES['_id']})
	except KeyError:
		k+=1

print "matches: "+str(a)+" misses: "+str(b)+" key errs: "+str(k)
