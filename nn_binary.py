from pymongo import Connection
from lib import sorting
import math
import sys
import random
import hashlib

conn=Connection()
db=conn.wc
nndb=conn.neuralweights
sys.setrecursionlimit(2000)
def returnWeight(SEARCHREC):
	rs1=nndb['binsettings'].find_one({'_id':'WikiNN'})
	try:
		VALUE=rs1[SEARCHREC]
	except:
		VALUE=1.0
	return VALUE
def nH1Eval(cHR,mHR):
	TOTAL=mHR-cHR
	if TOTAL<1 and TOTAL>0:
		return 0
	else:
		return 1
def nH2Eval(cHR,mHR2):
	TOTAL= mHR2-cHR
	if TOTAL<1 and TOTAL>0:
		return 0
	else:
		return 1

def nTodayEval(cTR,mTR):
	TOTAL= mTR-cTR
	if TOTAL<1 and TOTAL>0:
		return 0
	else:
		return 1
def nD1Eval(D1RAT,mRAT):
	TOTAL=mRAT-D1RAT
	if TOTAL<1 and TOTAL>0:
		return 0
	else:
		return 1
def nD2Eval(D2RAT,mRAT):
	TOTAL=mRAT-D2RAT
	if TOTAL<1 and TOTAL>0:
		return 0
	else:
		return 1
def nD3Eval(D3RAT,mRAT):
	TOTAL=mRAT-D3RAT
	if TOTAL<1 and TOTAL>0:
		return 0
	else:
		return 1
 
def setWeight(nVALUE,nTITLE):
	nndb['binsettings'].update({'_id':'WikiNN'},{'$set':{nTITLE:nVALUE}},upsert=True)
	return

def isLowest(REC,idx):
	LOWEST=''
	LVAL=99999
	if REC[0]<LVAL:
		LOWEST='H1S'
		LVAL=REC[0]
	if REC[1]<LVAL:
		LOWEST='H2S'
		LVAL=REC[1]
	if REC[2]<LVAL:
		LOWEST='T1S'
		LVAL=REC[2]
	if REC[3]<LVAL:
		LOWEST='D1'
		LVAL=REC[3]
	if REC[4]<LVAL:
		LOWEST='D2'
		LVAL=REC[4]
	if LOWEST==str(idx):
		return True
	else:
		return False
def isHighest(REC,idx):
	HIGHEST=''
	HVAL=-999
	if REC[0]>HVAL:
		HIGHEST='H1S'
		HVAL=REC[0]
	if REC[1]>HVAL:
		HIGHEST='H2S'
		HVAL=REC[1]
	if REC[2]>HVAL:
		HIGHEST='T1S'
		HVAL=REC[2]
	if REC[3]>HVAL:
		HIGHEST='D1'
		HVAL=REC[3]
	if REC[4]>HVAL:
		HIGHEST='D2'
		HVAL=REC[4]
	if HIGHEST==str(idx):
		return True
	else:
		return False
def bpnnRescoreWeights(RESULT,ScoreRecord):
	MVAL=0.95
	LVAL=1.002
	CUTOFF=2
	if RESULT=='y' or RESULT=='Y':
		#reward low scorers
		if ScoreRecord[0]<CUTOFF:
			H1SWeight=returnWeight('H1SWeight')
			H1SWeight=float(H1SWeight)*MVAL
			setWeight(H1SWeight,'H1SWeight')		
		if ScoreRecord[1]<CUTOFF:
			H2SWeight=returnWeight('H2SWeight')
			H2SWeight=float(H2SWeight)*MVAL
			setWeight(H2SWeight,'H2SWeight')
		if ScoreRecord[2]<CUTOFF:
			T1SWeight=returnWeight('T1SWeight')
			T1SWeight=float(T1SWeight)*MVAL
			setWeight(T1SWeight,'T1SWeight')
		if ScoreRecord[3]<CUTOFF:
			D1Weight=returnWeight('D1Weight')
			D1Weight=float(D1Weight)*MVAL
			setWeight(D1Weight,'D1Weight')
		if ScoreRecord[4]<CUTOFF:
			D2Weight=returnWeight('D2Weight')
			D2Weight=float(D2Weight)*MVAL
			setWeight(D2Weight,'D2Weight')
		if ScoreRecord[5]<CUTOFF:
			D3Weight=returnWeight('D3Weight')
			D3Weight=float(D3Weight)*MVAL
			setWeight(D3Weight,'D3Weight')
	elif RESULT=='n' or RESULT=='N':
		#reward high scorer
		if ScoreRecord[0]>CUTOFF:
			H1SWeight=returnWeight('H1SWeight')
			H1SWeight=float(H1SWeight)*LVAL
			setWeight(H1SWeight,'H1SWeight')		
		if ScoreRecord[1]>CUTOFF:
			H2SWeight=returnWeight('H2SWeight')
			H2SWeight=float(H2SWeight)*LVAL
			setWeight(H2SWeight,'H2SWeight')
		if ScoreRecord[2]>CUTOFF:
			T1SWeight=returnWeight('T1SWeight')
			T1SWeight=float(T1SWeight)*LVAL
			setWeight(T1SWeight,'T1SWeight')
		if ScoreRecord[3]>CUTOFF:
			D1Weight=returnWeight('D1Weight')
			D1Weight=float(D1Weight)*LVAL
			setWeight(D1Weight,'D1Weight')
		if ScoreRecord[4]>CUTOFF:
			D2Weight=returnWeight('D2Weight')
			D2Weight=float(D2Weight)*LVAL
			setWeight(D2Weight,'D2Weight')
		if ScoreRecord[5]>CUTOFF:
			D3Weight=returnWeight('D3Weight')
			D3Weight=float(D2Weight)*LVAL
			setWeight(D3Weight,'D3Weight')
	
				
	return
def calcMasterRatios(MASTERREC):
	#Save time by doing these calcs only once
	rsD=db['en_hitshourly'].find_one({'_id':MASTERREC})
	rsT=db['en_hitshourlydaily'].find_one({'_id':MASTERREC})
	rsM=db['en_hitsdaily'].find_one({'_id':MASTERREC})
	
	STRINGDATE='2013_10_07'
	YSTRINGDATE='2013_10_06'
	try:
		d3RAT=float(rsM[STRINGDATE])/float(rsM[YSTRINGDATE])
	except:
		d3RAT=1.0
	
        STRINGDATE="2013_10_06"
        YSTRINGDATE="2013_10_05"
	try:
		d2RAT=float(rsM[STRINGDATE])/float(rsM[YSTRINGDATE])
	except:
		d2RAT=1.0

        STRINGDATE="2013_10_03"
        YSTRINGDATE="2013_10_02"
	try:
		d1RAT=float(rsM[STRINGDATE])/float(rsM[YSTRINGDATE])
	except:
		d1RAT=1.0
	
	try:
		h1HR=float(rsD['16'])/float(rsD['23'])
	except:
		h1HR=1.0
	try:
		h2HR=float(rsD['08'])/float(rsD['16'])
	except:
		h2HR=1.0
	try:
		mTR=float(rsT['12'])/float(rsT['11'])
	except:
		mTR=1.0
	VLIST=(mTR,h2HR,h1HR,d1RAT,d2RAT,d3RAT)
	return VLIST
def calcChildRatios(id):
	rsD=db['en_hitsdaily'].find_one({'_id':id})
	rsT=db['en_hitshourlydaily'].find_one({'_id':id})
	rsH=db['en_hitshourly'].find_one({'_id':id})
	try:
		HRAT1=float(rsH['16'])/float(rsH['23'])
	except:
		HRAT1=0
	try:
		HRAT2=float(rsT['08'])/float(rsT['16'])
	except:
		HRAT2=0
	try:
		TRATIO=float(rsD['12'])/float(rsD['11'])
	except:
		TRATIO=0
        STRINGDATE="2013_10_03"
        YSTRINGDATE="2013_10_02"
        try:
        	D1RAT=float(rsD[STRINGDATE])/float(rsD[YSTRINGDATE])
        except:
        	D1RAT=0

        STRINGDATE="2013_10_06"
        YSTRINGDATE="2013_10_05"
        try:
        	D2RAT=float(rsD[STRINGDATE])/float(rsD[YSTRINGDATE])
        except:
        	D2RAT=0
	STRINGDATE='2013_10_07'
	YSTRINGDATE='2013_10_06'
	try:
		D3RAT=float(rsD[STRINGDATE])/float(rsD[YSTRINGDATE])
	except:
		D3RAT=0
	CLIST=(HRAT1,HRAT2,TRATIO,D1RAT,D2RAT,D3RAT)
	return CLIST

def getAllWeights():
	RS=nndb['binsettings'].find_one({'_id':'WikiNN'})
	return RS
	
def outputFunction(clirec,VLIST,ALLWEIGHTS):
	try:
		H1SWeight=ALLWEIGHTS['H1SWeight']
	except:
		H1SWeight=1
	try:
		H2SWeight=ALLWEIGHTS['H2SWeight']
	except:
		H2SWeight=1
	try:
		D1Weight=ALLWEIGHTS['D1Weight']
	except:
		D1Weight=1
	try:
		D2Weight=ALLWEIGHTS['D2Weight']
	except:
		D2Weight=1
	try:
		D3Weight=ALLWEIGHTS['D3Weight']
	except:
		D3Weight=1
	try:
		T1SWeight=ALLWEIGHTS['T1SWeight']
	except:
		T1SWeight=1
	
	CLIST=calcChildRatios(clirec['_id'])
	H1S=nH1Eval(CLIST[0],VLIST[2])*H1SWeight
	H2S=nH2Eval(CLIST[1],VLIST[1])*H2SWeight
	T1S=nTodayEval(CLIST[2],VLIST[0])*T1SWeight
	D1=nD1Eval(CLIST[3],VLIST[3])*D1Weight
	D2=nD2Eval(CLIST[4],VLIST[4])*D2Weight
	D3=nD3Eval(CLIST[5],VLIST[5])*D3Weight

#	print H1S,H2S,T1S,D1,D2,D3
	OUTPUT=math.fabs(H1S)+math.fabs(H2S)+math.fabs(T1S)+math.fabs(D1)+math.fabs(D2)+math.fabs(D3)
	WeightsRecord=(H1S,H2S,T1S,D1,D2,D3)
	return  OUTPUT,WeightsRecord

def main_ProductionRun():
	MRS=db['en_threehour'].find()
	RS=db['en_hitsdaily'].find({'2013_10_07':{'$gt':500}})
	ALLWEIGHTS=getAllWeights()
	for mrec in MRS:
		MASTERREC=mrec['_id']
		VLIST=calcMasterRatios(MASTERREC)
		MASTERTITLE=mrec['title']
		RL=[]
		print '[main_Production] Producing similars for '+str(MASTERTITLE)
		for clirec in RS:
			TOTALSCORE,WeightsRecord=outputFunction(clirec,VLIST,ALLWEIGHTS)
			insme=(TOTALSCORE,clirec['_id'],clirec['title'])
			RL.append(insme)
		print '[main_Production] now sorting list of '+str(len(RL))
		SL=sorting.QuickSort(RL)
		C=0
		for c in range(0,5):
			print SL[c]
		RS.rewind()
		print "---------------------------------"
	return
		
def main_TestRandomPages():
	RS=db['en_hitsdaily'].find({'2013_10_07':{'$gt':500}})
	CHECKS=db['en_hitsdaily'].find({'2013_10_07':{'$gt':500}})
	YESS=0
	NOS=0
	ALLWEIGHTS=getAllWeights()
	for c in range(0,25):
		SEED=random.randint(0,CHECKS.count())
		MASTERREC=CHECKS[SEED]['_id'] 
		VLIST=calcMasterRatios(MASTERREC)
		MASTERTITLE=CHECKS[SEED]['title']
		RL=[]
		print "Looking for similars to : "+str(MASTERTITLE)
		for clirec in RS:
			TOTALSCORE,WeightsRecord=outputFunction(clirec,VLIST,ALLWEIGHTS)
			insme=(TOTALSCORE,clirec['_id'],clirec['title'])
			RL.append(insme)
		print 'now sorting list of length '+str(len(RL))	
		SL=sorting.QuickSort(RL)
		#SL=sorted(RL)
		C=0
		for a in SL:
			if a[0]>1:
				continue
			print a
			uinput=raw_input("Master: "+str(MASTERTITLE)+". Is this a match?(Y/N)")
			bpnnRescoreWeights(str(uinput),WeightsRecord)
		RS.rewind()
	return

def main_TestSinglePage():
	LOWERBOUND=0.402
	UPPERBOUND=0.4462
	RS=db['en_hitsdaily'].find({'2013_10_07':{'$gt':500}})
	MASTERREC=hashlib.sha1('Breaking_Bad').hexdigest() 
	VLIST=calcMasterRatios(MASTERREC)
	MASTERTITLE='Breaking Bad'
	ALLWEIGHTS=getAllWeights()
	RL=[]
	print "Looking for similars to : "+str(MASTERTITLE)
	for clirec in RS:
		TOTALSCORE,WeightsRecord=outputFunction(clirec,VLIST,ALLWEIGHTS)
		insme=(TOTALSCORE,clirec['_id'],clirec['title'])
		RL.append(insme)
	print 'now sorting list of length '+str(len(RL))	
	SL=sorting.QuickSort(RL)
	#SL=sorted(RL)
	C=0
	for a in SL:
		if a[0]< LOWERBOUND or a[0]> UPPERBOUND:
			continue
		print str(a)+'\r'
		uinput=raw_input("Master: "+str(MASTERTITLE)+". Is this a match?(Y/N)")
		bpnnRescoreWeights(str(uinput),WeightsRecord)
	return

def main_SingleOFILE():
	HASHSTRING='Green_Bay_Packers'
	RS=db['en_hitsdaily'].find({'2013_10_07':{'$gt':500}})
	MASTERREC=hashlib.sha1(HASHSTRING).hexdigest()
	VLIST=calcMasterRatios(MASTERREC)
	ALLWEIGHTS=getAllWeights()
	RL=[]
	print "Scoring for similars to "+str(HASHSTRING)
	for clirec in RS:
		TOTALSCORE,WeightsRecord=outputFunction(clirec,VLIST,ALLWEIGHTS)
		insme=(TOTALSCORE,clirec['title'])
		RL.append(insme)
	print "now sorting"
	SL=sorting.QuickSort(RL)
	OFILE=open(MASTERREC+'.txt','w')
	for a in SL:
		OFILE.write(str(a)+'\n')
	OFILE.close()
	return

#main_SingleOFILE()	
#main_TestSinglePage()
#main_ProductionRun()
main_TestRandomPages()
