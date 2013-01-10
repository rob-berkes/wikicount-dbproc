from pymongo import Connection
conn=Connection()
db=conn.wc
for item in db.map.find():
     if db.hits.find_one({'_id':item['_id']}):
             pass
     else:
             db.map.remove({'_id':item['_id']})
