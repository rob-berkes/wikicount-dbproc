from pymongo import Connection
from functions import wikicount
DAY, MONTH, YEAR,HOUR, expiretime=wikicount.fnReturnTimes()
conn=Connection()
db=conn.wc
LANGUAGES=wikicount.getLanguageList()
for lang in LANGUAGES:
	CNAME=str(lang)+'_hitshourly'
	RESULT=db[CNAME].drop()
	print lang, str(RESULT)
