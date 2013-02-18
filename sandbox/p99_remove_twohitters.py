#!/usr/bin/python
from pymongo import Connection
from multiprocessing import Process

conn=Connection()
db=conn.wc

def f(OFFSET,OUTTEXT):
	QUERY=db.hits.find({'Hits':{'$lt':6}}).limit(20000).skip(OFFSET)
	for line in QUERY:
		db.map.remove({'_id':line['_id']})
		db.hits.remove({'_id':line['_id']})
	print OUTTEXT


if __name__ == '__main__':
    p = Process(target=f, args=(0,'First 10000'))
    q = Process(target=f, args=(20000,'Second 10000'))
    r = Process(target=f, args=(40000,'Third 10000'))
    s = Process(target=f, args=(60000,'Fourth 10000'))
#    t = Process(target=f, args=(40000,'Fifth 10000'))
	

    p.daemon=True
    q.daemon=True
    r.daemon=True
    s.daemon=True
#    t.daemon=True

    p.start()
    q.start()
    r.start()
    s.start()
#    t.start()

    p.join()
    q.join()
    r.join()
    s.join()
#   t.join()

