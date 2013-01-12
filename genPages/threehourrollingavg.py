from pymongo import Connection
from functions import wikicount
from numpy import sqrt,mean,array
from operator import itemgetter
import syslog
conn=Connection()
db=conn.wc

DAY,MONTH,YEAR,HOUR,expiretimes = wikicount.fnReturnTimes()
HOUR=int(HOUR)-1
if HOUR==-1:
	HOUR=23
print HOUR
HOUR=1
HOUR2=HOUR-1
HOUR3=HOUR2-1
if HOUR2==-1:
	HOUR2=23
if HOUR3==-1:
	HOUR3=23
elif HOUR3==-2:
	HOUR3=22

HOUR='%02d' % (HOUR,)
HOUR2='%02d' % (HOUR2,)
HOUR3='%02d' % (HOUR3,)
ALLRES=db.hitshourly.find({str(HOUR):{'$gt':15}}).sort(str(HOUR),-1).limit(50000)
hourlies=[]
TypeErrors=0
KeyErrors=0
for item in ALLRES:
	try:
		QUERYtitle=db.hits.find_one({'_id':item['_id']})
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
