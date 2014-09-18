from pymongo import Connection
from lib import wikicount

DAY, MONTH, YEAR,HOUR, expiretime= wikicount.fnReturnTimes()
conn=Connection()
db=conn.wc
LANGUAGES= wikicount.getLanguageList()
for lang in LANGUAGES:
	HHNAME=str(lang)+'_hitsdaily'
	QUERYA=db[HHNAME].update({'2013_09_12':{'$exists':True}},{'$set':{'2013_09_12':1}},False,True)
