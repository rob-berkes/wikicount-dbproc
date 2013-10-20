from pymongo import Connection
from datetime import time

class Hour:
	hour=0
	hits=0
	h2nratio=0
	hourscore=0
	hm1score=0
	hp1score=0
	def __init__(self):
		hour=0
		h2nratio=float(0)
		hourscore=float(0)
		hm1score=float(0)
		hp1score=float(0)
	def calcratio(DIVISOR):
		return float(hits/DIVISOR)
	def calcscore(SCORE,DIVISOR):
		return float(1/(SCORE-DIVISOR))

conn=Connection()
db=conn.wc

def getAllHourHits(IDREC):
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
TODAY=getAllHourHits('a2976cc2ec4ddbd09e87da88f65b6df552d850eb')
WEIGHTLIST=getAllHourWeights('a2976cc2ec4ddbd09e87da88f65b6df552d850eb')

for hour in TODAY:
	print str(hour.hour)+" : "+str(hour.hits)
