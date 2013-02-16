from pymongo import Connection
conn=Connection()
db=conn.wc
db.hitshourlydaily.remove()

