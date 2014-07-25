#/usr/bin/python
import syslog

from pymongo import Connection
from lib import wikicount


STARTTIME= wikicount.fnStartTimer()
syslog.syslog('tophits.py:  starting...')
DAY,MONTH,YEAR,HOUR,expiretime= wikicount.fnReturnTimes()
DAY,MONTH,HOUR= wikicount.fnFormatTimes(DAY,MONTH,HOUR)
MONTHNAME= wikicount.fnGetMonthName()
HOUR= wikicount.minusHour(int(HOUR))
conn=Connection()
db=conn.wc
RECCOUNT=1
DAYKEY=str(YEAR)+"_"+str(MONTH)+"_"+str(DAY)
PLACEMAP="hitsplacemap"

wikicount.fnSetStatusMsg('tophits',0)

LANGLIST= wikicount.getLanguageList()
for lang in LANGLIST:
	PLACEMAP=str(lang)+"_mapPlace"
	HITSMAP=str(lang)+"_mapHits"
	try:
		IFILE=open("/home/ec2-user/"+str(lang)+"_mongo.csv.sorted","r")
	except IOError:
		syslog.syslog("Error opening file for "+str(lang))
		continue
	RESULT=[]
	RECCOUNT=0
	for line in IFILE:
    		if RECCOUNT < 1000:
        		line=line.strip().split(",")
        		RESULT.append((line[0],line[1]))
        		RECCOUNT+=1
        		try:
				db[PLACEMAP].update({'_id':str(line[1])},{"$set":{DAYKEY:RECCOUNT}},upsert=True)
				db[HITSMAP].update({'_id':str(line[1])},{"$set":{DAYKEY:int(line[0])}},upsert=True)
			except TypeError:
    				pass
   	 	else:
        		break
IFILE.close()

RUNTIME= wikicount.fnEndTimerCalcRuntime(STARTTIME)
syslog.syslog('tophits.py: runtime is '+str(RUNTIME)+' seconds.')
wikicount.fnSetStatusMsg('tophits',3)
wikicount.fnLaunchNextJob('tophits')
