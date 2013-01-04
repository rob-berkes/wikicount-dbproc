#!/usr/bin/python
from pymongo import Connection
from functions import wikicount
conn=Connection()
db=conn.wc 

QUERY=db.hitshourly.find().limit(10)
for item in QUERY:
	name=wikicount.MapQuery_FindName(item['_id'])
	print name

