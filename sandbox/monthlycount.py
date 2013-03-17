from pymongo import Connection 
from functions import wikicount

conn=Connection()
db=conn.wc

DAY,MONTH,YEAR,HOUR,expiretime=wikicount.fnReturnTimes()
DAY,MONTH,HOUR=wikicount.fnFormatTimes(DAY,MONTH,HOUR)

SEARCHQ=db.hitsdaily.find()

for line in SEARCHQ:
	HITS=0
	for DAY in range(1,31):
		DAYFMT='%02d'%(int(DAY),)
		SEARCHDAY='2013_02_'+str(DAYFMT)
		try:
			HITS+=line[SEARCHDAY]
		except:
			pass
	if HITS > 0:
		db.monthcount.update({"_id":line['_id']},{'$inc':{'2013_02':HITS}},upsert=True)
