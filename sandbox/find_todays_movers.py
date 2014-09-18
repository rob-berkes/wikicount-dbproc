from lib import sorting
from multiprocessing import Pool
from pymongo import Connection
import time
def f(A):
	RL=[]
	for i in A:
		try:
			rec=(i[0]-i[1],i[2],i[3])
			RL.append(rec)
		except:
			pass
	return RL
def makeList(RS):
	RL=[]
	for a in RS:
		try:
			rec=(a['2013_10_03'],a['2013_10_02'],a['_id'],a['title'])
			RL.append(rec)
		except:
			pass
	return RL
st=time.time()
conn=Connection()
db=conn.wc
TBL='en_hitsdaily'
RS=db['en_hitsdaily'].find({'2013_10_03':{'$gt':10}})
print "RS found in "+str(time.time()-st)+" seconds. Now making list..."
st=time.time()
RL=makeList(RS)
print"RL made in "+str(time.time()-st)+" seconds. Now diffing list..."
st=time.time()
DRS=f(RL)	
print "Diffed list of "+str(len(DRS))+" records returned in  "+str(time.time()-st)+" seconds."
st=time.time()
SL=sorting.QuickSortListArray(DRS)
print "Diffed List sorted in "+str(time.time()-st)+" seconds."
for C in range(1,25):
	if C>11:
		break
	print SL[-C]
	C+=1

