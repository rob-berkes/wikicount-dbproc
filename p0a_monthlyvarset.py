from pymongo import Connection
from functions import wikicount
conn=Connection()
db=conn.wc
DAY,MONTH,YEAR,HOUR,expiretime=wikicount.fnReturnTimes()
MONTHNAME=wikicount.fnGetMonthName()

THTABLENAME='tophits'+str(YEAR)+str(MONTHNAME)
#DBTABLENAME='proddebuts'+str(YEAR)+str(MONTHNAME)
db[THTABLENAME].ensureIndex({"id":1})
db[THTABLENAME].ensureIndex({"d":1,"m":1,"y":1})

