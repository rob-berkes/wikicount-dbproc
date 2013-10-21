from pymongo import Connection
from datetime import time
from lib import sorting 
conn=Connection()
db=conn.wc

class Weights:
	hour=0
	weight=0.0
class Scores:
	hratio=0
	hm1ratio=0
	hp1ratio=0
	hourscore=0
	hm1score=0
	hp1score=0
	def __init__(self):
		hratio=0
		hm1ratio=0
		hp1ratio=0
	def __init__(self,Hits,mHOUR,h2n,MASTERHOUR):
		hratio=float(Hits/MASTERHOUR.HOURS[mHOUR].hits)
		if mHOUR==0:
			mnHOUR=23
		else:
			mnHOUR-=1
		hm1ratio=float(Hits/MASTERHOUR.HOUR[mnHOUR].hits)
class Hour:
	hour=0
	hits=0
	h2nratio=0
	def __init__(self):
		hour=0
		h2nratio=float(0)
		hourscore=float(0)
		hm1score=float(0)
		hp1score=float(0)
	def __init__(self,HITS,MASTERHOUR,HOUR):
		hour=HOUR
		hits=HITS
		h2nratio=float(hits/MASTERHOUR.Hours[HOUR].hits)
		SCORES=SCORES(hits,hour,h2nratio,MASTERHOUR)
	def calcratio(DIVISOR):
		return float(hits/DIVISOR)
	def calcscore(SCORE,DIVISOR):
		return float(1/(SCORE-DIVISOR))
class HourList:
	id=''
	title=''
	HOURS=[]
	WEIGHTS=[]

def getMastHourHits(IDREC):
	DAY=[]
	rs=db['en_hitshourly'].find_one({'_id':IDREC})
	for hour in range(0,24):
		a=Hour()
		a.hour=hour
		HOURSTR="%02d" %(hour,)
		try:
			a.hits=rs[HOURSTR]
		except KeyError:
			a.hits=0
		DAY.append(a)
	return DAY
def getAllHourHits(IDREC,MASTERHOURSET):
	DAY=[]
	rs=db['en_hitshourly'].find_one({'_id':IDREC})
	for hour in range(0,24):
		HOURSTR="%02d" % (hour,)
		a=Hour(rs[HOURSTR],MASTERHOURSET.Hours[hour],hour)
		
def getAllHourWeights(IDREC):
	WEIGHTLIST=[]
	rs=db['en_similars_hour_weights'].find_one({'_id':IDREC})
	for hour in range(0,24):
		try:
			W=rs[hour]
		except TypeError:
			W=1.0
		WEIGHTLIST.append(W)
	return WEIGHTLIST



def main_CompareTo25():
	SDATE='12'
	MASTERREC='a2976cc2ec4ddbd09e87da88f65b6df552d850eb'
	MASTERHOURSET=HourList()
	MASTERHOURSET.Weights=getAllHourWeights(MASTERREC)
	MASTERHOURSET.Hours=getMastHourHits(MASTERREC)
	for hour in TODAY:
		print str(hour.hour)+" : "+str(hour.hits)
	
	SQUERY={SDATE:{'$gt':300}}
	RSET=db['en_hitshourly'].find(SQUERY).limit(25)
	FULLHOURSET=[]
	for rec in RSET:
		rs=db['en_hitsdaily'].find_one({'_id':rec['_id']})
		HOURSET=HourList()
		HOURSET.id=rec['_id']
		HOURSET.title=rs['title']
		HOURSET.HOURS=getAllHourHits(rec['_id'],MASTERHOURSET)
		FULLHOURSET.append(HOURSET)
	print len(FULLHOURSET)
	for rec in FULLHOURSET:
		print rec.title
		for h in rec.HOURS:
			print h.hour,h.hits
	return



main_CompareTo25()
