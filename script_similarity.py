from pymongo import Connection
from lib import sorting
import math
import time

ID='cb7193d384b95c6f30fc49bb12861db349788e8f'
conn=Connection()
db=conn.wc
lang='en'
HD=str(lang)+'_hitsdaily'

RCURS=db[HD].find_one({'_id':ID})
class hitRecord:
	year=2013
	month=1
	day=1
	hits=0

DATE1="2013_09_18"
DATE2="2013_09_22"
DATE3="2013_09_25"

D1Q=db[HD].find_one({'_id':ID})
D1SCORE=D1Q[DATE1]
D2SCORE=D1Q[DATE2]
D3SCORE=D1Q[DATE3]

def makeArray(LLIST):
	NEWARRAY=[]
	for i in LLIST:
		rec=(i['TOTAL'],i['_id'],i['title'])
		NEWARRAY.append(rec)
	return NEWARRAY
	
def getDayList():
	print 'building array'
	RSET=db[HD].find({DATE1:{'$exists':True}})
	RLIST=[]
	for r in RSET:
		try:
			rec={'_id':r['_id'],'title':r['title'],DATE1:r[DATE1],DATE2:r[DATE2],DATE3:r[DATE3]}
			#rec={'_id':r['_id'],DATE1:r[DATE1],DATE2:r[DATE2]}
			if r['_id']!=ID:
				RLIST.append(rec)
		except NameError:
			pass
		except KeyError:
			pass
	return RLIST
def scoreList(D1SCORE,D2SCORE,MATCHLIST):
	print 'scoring array'
	NLIST=[]
	for m in MATCHLIST:
		try:
			D1DIFF=math.fabs(D1SCORE-m[DATE1])
			D2DIFF=math.fabs(D2SCORE-m[DATE2])
			D3DIFF=math.fabs(D3SCORE-m[DATE3])
			TOTDIFF=D1DIFF+D2DIFF+D3DIFF
			rec={'_id':m['_id'],'title':m['title'],'D1DIFF':D1DIFF,'D2DIFF':D2DIFF,'D3DIFF':D3DIFF,'TOTAL':TOTDIFF}
			NLIST.append(rec)
		except KeyError:
			pass
	return NLIST

def findSmallest(LLIST):
	SMALLEST=[]
	HITS=999999 
 	HITS2=999999
	for rec in LLIST:
		if rec['TOTAL'] < HITS:
			HITS2=HITS
			HITS=rec['TOTAL']
			SECSMALL=SMALLEST
			SMALLEST=[]
			SMALLEST.append(rec)
		elif rec['TOTAL'] == HITS:
			SMALLEST.append(rec)
		elif rec['TOTAL'] == HITS2:
			SECSMALL.append(rec)
	return SMALLEST,SECSMALL



STARTTIME=time.time()
MATCHLIST=getDayList()
ENDTIME=time.time()
TTIME=ENDTIME-STARTTIME
print "found "+ str(len(MATCHLIST))+" records in "+str(TTIME)+" seconds, now scoring..."

STIME=time.time()
NEWLIST=scoreList(D1SCORE,D2SCORE,MATCHLIST)
ETIME=time.time()
TTIME=ETIME-STIME
print str(len(NEWLIST))+" items scored in "+str(TTIME)+" seconds. finding smallest match"

STIME=time.time()
NEWARRAY=makeArray(NEWLIST)
ETIME=time.time()
TTIME=ETIME-STIME
print 'array made in '+str(TTIME)+' seconds. Now quicksorting'

STIME=time.time()
SORTLIST=sorting.QuickSortListArray(NEWARRAY)
ETIME=time.time()
TTIME=ETIME-STIME
print 'sorted list returned in '+str(TTIME)+' seconds. Writing to file.'

OFILE=open('sorted.list','w')
for rec in SORTLIST:
	OFILE.write(str(rec[0])+' '+str(rec[1])+' '+str(rec[2]))
	OFILE.write('\n')
OFILE.close()


#STIME=time.time()
#SMALL_LIST,SECSMALL=findSmallest(NEWLIST)
#TTIME=time.time()
#ETIME=TTIME-STIME


#print 'found '+str(len(SMALL_LIST))+' closest matches and '+str(len(SECSMALL))+' next closest in '+str(ETIME)+' seconds.'

#print SMALL_LIST


#print SECSMALL
