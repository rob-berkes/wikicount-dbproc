from pymongo import Connection
from functions import wikicount
from numpy import sqrt,mean,array
from operator import itemgetter
import syslog

STARTTIME=wikicount.fnStartTimer()
conn=Connection()
db=conn.wc
DAY,MONTH,YEAR,HOUR,expiretimes = wikicount.fnReturnTimes()
HOUR=wikicount.minusHour(int(HOUR))
HOUR,HOUR2,HOUR3=wikicount.fnReturnLastThreeHours(HOUR)
HOUR=wikicount.fnStrFmtDate(HOUR)
HOUR2=wikicount.fnStrFmtDate(HOUR2)
HOUR3=wikicount.fnStrFmtDate(HOUR3)
wikicount.fnSetStatusMsg('threehrrollingavg',0)
ALLRES=db.hitshourlydaily.find({str(HOUR):{'$exists':True}}).sort(str(HOUR),-1).limit(50000)
hourlies=[]
TypeErrors=0
KeyErrors=0
for item in ALLRES:
	try:
		QUERYtitle=db.hitsdaily.find_one({'_id':item['_id']})
		atitle=QUERYtitle['title']
		title,utitle=wikicount.FormatName(atitle)
		try:
			b1=item[HOUR]
		except KeyError:
			b1=0
		try:
			b2=item[HOUR2]
		except KeyError:
			b2=0
		try:
			b3=item[HOUR3]
		except KeyError:
			b3=0
	
		rollingavg=mean(array([b1,b2,b3]))

		rec={'title':title,'rollavg':int(rollingavg),'id':item['_id']}
		hourlies.append(rec)
	except TypeError:
		TypeErrors+=1
	except KeyError:
		KeyErrors+=1

syslog.syslog("[3hrrollavg] - TypeErrors: "+str(TypeErrors)+" KeyErrors: "+str(KeyErrors))

z=1
db.threehour.remove()
for w in sorted(hourlies,key=itemgetter('rollavg'),reverse=True):
	if z < 101:
		rec={'place':z,'title':w['title'],'rollavg':w['rollavg'],'id':w['id']}
		db.threehour.insert(rec)
		z+=1
RUNTIME=wikicount.fnEndTimerCalcRuntime(STARTTIME)
syslog.syslog('threehourrollingavg.py :  runtime is '+str(RUNTIME)+' seconds.')
wikicount.fnSetStatusMsg('threehrrollingavg',3)
wikicount.fnLaunchNextJob('threehrrollingavg')
