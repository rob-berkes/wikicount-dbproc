from pymongo import Connection
conn=Connection()
db=conn.wc
for item in db.map.find():
     if db.hits.find_one({'_id':item['_id']}):
	     TITLESTRING=item['title'].decode('utf-8')
	     POSTFIND={'_id': item['_id']}
             db.hits.update(POSTFIND,{"$set":{'title':TITLESTRING} },upsert=True)
	     db.map.remove(POSTFIND)
