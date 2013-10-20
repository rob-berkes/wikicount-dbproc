from pymongo import Connection

class Hour:
	hour=0
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
		return float(HOUR/DIVISOR)
	def calcscore(SCORE,DIVISOR):
		return float(1/(SCORE-DIVISOR))

conn=Connection()
db=conn.wc

