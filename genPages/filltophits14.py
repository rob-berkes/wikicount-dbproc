from pymongo import Connection 
conn=Connection()
db=conn.wc

QUERY={'d':14,'m':2,'y':2013}

for line in db.tophits.find(QUERY):
	db.tophits2013February.insert(line)
