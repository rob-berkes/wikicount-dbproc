
from pymongo import Connection
from datetime import time
from lib import sorting 
import math
conn=Connection()
db=conn.wc

class Weights:
	hour=0
	weight=0.0
class Scores:
	hratio=0
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
	hm1ratio=0
	hp1ratio=0
	hp1score=0
	hm1score=0
	def __init__(self):
		hour=0
		return
class HourList:
	id=''
	title=''
	HOURS=[]
	WEIGHTS=[]
	def calc_Ratios(self):
		for hour in self.HOURS:
			if hour.hour==0:
				prevhour=23
			else:
				prevhour=hour.hour-1
			if hour.hour==23:
				nexthour=0
			else:
				nexthour=hour.hour+1
			hour.hm1ratio=float(hour.hits)/self.HOURS[prevhour].hits
			hour.hp1ratio=float(hour.hits)/self.HOURS[nexthour].hits
		return
	def calc_Scores(self,MASTER):
		for a in range(0,24):
			self.HOURS[a].hp1score=1/(math.fabs(self.HOURS[a].hp1ratio-MASTER.HOURS[a].hp1ratio))
			self.HOURS[a].hm1score=1/(math.fabs(self.HOURS[a].hm1ratio-MASTER.HOURS[a].hm1ratio))
		return
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
			a.hits=1
		DAY.append(a)
	return DAY

def dumpHourSet(FULLHOURSET):
	for rec in FULLHOURSET:
		print rec.id,rec.title
		print "Hour Scores"
		for hour in rec.HOURS:
			print hour.hour,hour.hits,hour.hp1score,hour.hm1score
	return
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

def getAllHourHits(IDREC,MASTERHOURSET):
	rs=db['en_hitshourly'].find_one({'_id':IDREC})
	HOURLIST=[]
	for a in range(0,24):
		HOUR=Hour()
		HOUR.hour=a
		HOURSTR="%02d" % (a,)
		try:
			HOUR.hits=rs[HOURSTR]
		except KeyError:
			HOUR.hits=1
		HOURLIST.append(HOUR)
	return HOURLIST
		
def printGoodMatches(MASTER,FULL):
	LEVEL=1000
	for rec in FULL:
		for hour in rec.HOURS:
			if hour.hp1score  > LEVEL or hour.hm1score > LEVEL:
				print MASTER.title,rec.title,hour.hour,hour.hp1score,hour.hm1score,hour.hits
	return
		
def main_CompareTo25():
	SDATE='12'
	MASTERREC='a2976cc2ec4ddbd09e87da88f65b6df552d850eb'
	MASTERHOURSET=HourList()
	MASTERHOURSET.Weights=getAllHourWeights(MASTERREC)
	MASTERHOURSET.HOURS=getMastHourHits(MASTERREC)
	MASTERHOURSET.calc_Ratios()
	for hour in MASTERHOURSET.HOURS:
		print str(hour.hour)+" : "+str(hour.hits)+', '+str(hour.hm1ratio)+", "+str(hour.hp1ratio)
	
	SQUERY={SDATE:{'$gt':300}}
	RSET=db['en_hitshourly'].find(SQUERY).limit(2500)
	FULLHOURSET=[]
	for rec in RSET:
		rs=db['en_hitsdaily'].find_one({'_id':rec['_id']})
		HOURSET=HourList()
		HOURSET.id=rec['_id']
		HOURSET.title=rs['title']
		HOURSET.HOURS=getAllHourHits(rec['_id'],MASTERHOURSET)
		HOURSET.calc_Ratios()
		HOURSET.calc_Scores(MASTERHOURSET)
		FULLHOURSET.append(HOURSET)
	printGoodMatches(MASTERHOURSET,FULLHOURSET)
		
	return



main_CompareTo25()
