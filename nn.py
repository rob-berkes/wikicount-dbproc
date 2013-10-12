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
	rs1=nndb['settings'].find_one({'_id':'WikiNN'})
	try:
		VALUE=rs1[SEARCHREC]
	except:
		VALUE=1.0
	return VALUE
def nHourEval(cHR,mHR):
	return mHR-cHR
def nTodayEval(cTR,mTR):
	return mTR-cTR
def nDayEval(D1RAT,mRAT):
	return mRAT-D1RAT




def retMD5(name):
	return hashlib.sha1(name).hexdigest()
 
def setWeight(nVALUE,nTITLE):
	nndb['settings'].update({'_id':'WikiNN'},{'$set':{nTITLE:nVALUE}},upsert=True)
	return

def findLowest(REC,fromLowest):
	LOWEST=999999
	LOWNAME='Initvalue'
	for name,value in REC.iteritems():
		if value < LOWEST and value!=0 and value!=1:
			LOWEST=value
			LOWNAME=name
	return LOWNAME,LOWEST

def findHighest(REC,fromHighest):
	HIGHEST=-1
	HIGHNAME='InitValue'
	for name,value in REC.iteritems():
		if value > HIGHEST and value!=0 and value!=1:
			HIGHEST=value
			HIGHNAME=name
	return HIGHNAME,HIGHEST

def bpnnRescoreWeights(RESULT,WeightsRecord,ScoreRecord):
	MVAL=0.005
	LVAL=0.00001
	
	LOWNAME,LOWVALUE=findLowest(ScoreRecord,1)
	WLNAME=LOWNAME+'Weight'
	HIGHNAME,HIGHVALUE=findHighest(ScoreRecord,1)
	WHNAME=HIGHNAME+'Weight'
	NewLowWeight=WeightsRecord[WLNAME]-MVAL
	NewHighWeight=WeightsRecord[WHNAME]+LVAL
	if RESULT=='y' or RESULT=='Y':
		setWeight(NewLowWeight,WLNAME)
	elif RESULT=='n' or RESULT=='N':
		setWeight(NewHighWeight,WHNAME)
	# if neither y or n, do nothing
	return

def calcRatios(MASTERREC):
	#Save time by doing these calcs only once
	rsD=db['en_hitshourly'].find_one({'_id':MASTERREC})
	rsT=db['en_hitshourlydaily'].find_one({'_id':str(MASTERREC)})
	rsM=db['en_hitsdaily'].find_one({'_id':str(MASTERREC)})
	Misses=0
	STRINGDATE='2013_10_09'
	YSTRINGDATE='2013_08_29'
	try:
		d5RAT=float(rsM[STRINGDATE])/float(rsM[YSTRINGDATE])
	except:
		Misses+=1
		d5RAT=1
	STRINGDATE='2013_10_04'
	YSTRINGDATE='2013_08_29'
	try:
		d6RAT=float(rsM[STRINGDATE])/float(rsM[YSTRINGDATE])
	except:
		Misses+=1
		d6RAT=1

	STRINGDATE='2013_10_08'
	YSTRINGDATE='2013_10_01'
	try:
		d4RAT=float(rsM[STRINGDATE])/float(rsM[YSTRINGDATE])
	except:
		Misses+=1
		d4RAT=1.0
	STRINGDATE='2013_10_07'
	YSTRINGDATE='2013_10_06'
	try:
		d3RAT=float(rsM[STRINGDATE])/float(rsM[YSTRINGDATE])
	except:
		Misses+=1
		d3RAT=1.0
	
        STRINGDATE="2013_10_06"
        YSTRINGDATE="2013_10_05"
	try:
		d2RAT=float(rsM[STRINGDATE])/float(rsM[YSTRINGDATE])
	except:
		Misses+=1
		d2RAT=1.0

        STRINGDATE="2013_10_03"
        YSTRINGDATE="2013_10_02"
	try:
		d1RAT=float(rsM[STRINGDATE])/float(rsM[YSTRINGDATE])
	except:
		Misses+=1
		d1RAT=1.0
	
	try:
		h1HR=float(rsD['16'])/float(rsD['23'])
	except:
		Misses+=1
		h1HR=1.0
	try:
		h2HR=float(rsD['08'])/float(rsD['16'])
	except:
		Misses+=1
		h2HR=1.0
	try:
		h3HR=float(rsD['12'])/float(rsD['08'])
	except:
		Misses+=1
		h3HR=1.0
	try:
		mTR=float(rsT['12'])/float(rsT['11'])
	except:
		Misses+=1
		mTR=1.0
	VLIST=(mTR,h2HR,h1HR,d1RAT,d2RAT,d3RAT,h3HR,d4RAT,d5RAT,d6RAT)
	return VLIST,Misses

def getAllWeights():
	RS=nndb['settings'].find_one({'_id':'WikiNN'})
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
		H3SWeight=ALLWEIGHTS['H3SWeight']
	except:
		H3SWeight=1
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
		D4Weight=ALLWEIGHTS['D4Weight']
	except:
		D4Weight=1
	try:
		D5Weight=ALLWEIGHTS['D5Weight']
	except:
		D5Weight=1
	try:
		D6Weight=ALLWEIGHTS['D6Weight']
	except:
		D6Weight=1
	try:
		T1SWeight=ALLWEIGHTS['T1SWeight']
	except:
		T1SWeight=1
	
	CLIST,Misses=calcRatios(clirec['_id'])
	H1S=nHourEval(CLIST[0],VLIST[2])*H1SWeight
	H2S=nHourEval(CLIST[1],VLIST[1])*H2SWeight
	H3S=nHourEval(CLIST[6],VLIST[6])*H3SWeight
	T1S=nTodayEval(CLIST[2],VLIST[0])*T1SWeight
	D1=nDayEval(CLIST[3],VLIST[3])*D1Weight
	D2=nDayEval(CLIST[4],VLIST[4])*D2Weight
	D3=nDayEval(CLIST[5],VLIST[5])*D3Weight
	D4=nDayEval(CLIST[7],VLIST[7])*D4Weight
	D5=nDayEval(CLIST[8],VLIST[8])*D5Weight
	D6=nDayEval(CLIST[9],VLIST[9])*D6Weight
#	print H1S,H2S,T1S,D1,D2,D3
	OUTPUT=math.fabs(H1S)+math.fabs(H2S)+math.fabs(T1S)+math.fabs(D1)+math.fabs(D2)+math.fabs(D3)+math.fabs(H3S)+math.fabs(D4)+math.fabs(D5)+math.fabs(D6)
	WeightsRecord={'H1SWeight':H1SWeight,'H2SWeight':H2SWeight,'T1SWeight':T1SWeight,'D1Weight':D1Weight,'D2Weight':D2Weight,'D3Weight':D3Weight,'H3SWeight':H3SWeight,'D4Weight':D4Weight,'D5Weight':D5Weight,'D6Weight':D6Weight}
	ScoreRecord={'H1S':H1S,'H2S':H2S,'T1S':T1S,'D1':D1,'D2':D2,'D3':D3,'H3S':H3S,'D4':D4,'D5':D5,'D6':D6}
	return  OUTPUT,WeightsRecord,ScoreRecord,Misses

def main_InsertIntoDB():
	MRS=db['en_threehour'].find()
	RS=db['en_hitsdaily'].find({'2013_10_10':{'$gt':500}})
	ALLWEIGHTS=getAllWeights()
	SKIPS=0
	for mrec in MRS:
		EJECTIONS=0
		MASTERREC=mrec['id']
		VLIST,MMisses=calcRatios(MASTERREC)
		if MMisses>4:
			SKIPS+=1
			continue
		MASTERTITLE=mrec['title']
		RL=[]
		EJECTIONS=0
		print '[main_InsertIntoDB] Producing similars for '+str(MASTERTITLE)
		for clirec in RS:
			TOTALSCORE,WeightsRecord,ScoreRecord,Misses=outputFunction(clirec,VLIST,ALLWEIGHTS)
			if Misses <3:
				insme=(TOTALSCORE,clirec['_id'],clirec['title'])
				RL.append(insme)
			else:
				EJECTIONS+=1
		print '[main_InsertIntoDB] Insert complete, '+str(EJECTIONS)+' ejections. now sorting list of size '+str(len(RL))
		SL=sorting.QuickSort(RL)
		COUNT=0
		ZCOUNT=0
		ILIST=[]
		for s in SL:
			if s[0]!=0:
				ID=str(s[1])
				IREC={'score':s[0],'id':s[1],'title':s[2]}
				ILIST.append(IREC)
				COUNT+=1
			else:
				ZCOUNT+=1
			if COUNT > 5:
				break
		print '[main_InsertIntoDB] List sorted. '+str(ZCOUNT)+' records were skipped due to being zero exactly.'
		INSERTREC={'_id':MASTERREC,'similars':ILIST}
		db['en_similarity'].insert(INSERTREC)
		RS.rewind()
	return	
				
def main_ProductionRun():
	MRS=db['en_threehour'].find()
	RS=db['en_hitsdaily'].find({'2013_10_10':{'$gt':500}})
	ALLWEIGHTS=getAllWeights()
	SKIPS=0
	for mrec in MRS:
		MASTERREC=mrec['id']
		VLIST,MMisses=calcRatios(MASTERREC)
		if MMisses>4:
			SKIPS+=1
			continue
		MASTERTITLE=mrec['title']
		RL=[]
		EJECTIONS=0
		print '[main_Production] Producing similars for '+str(MASTERTITLE)
		for clirec in RS:
			TOTALSCORE,WeightsRecord,ScoreRecord,Misses=outputFunction(clirec,VLIST,ALLWEIGHTS)
			if Misses<3:
				insme=(TOTALSCORE,clirec['_id'],clirec['title'])
				RL.append(insme)
			else:
				EJECTIONS+=1
		print '[main_Production] now sorting list of '+str(len(RL))
		SL=sorting.QuickSort(RL)
		C=0
		Z=0
		for s in SL:
			if s[0]!=0:
				print s
				C+=1
			else:
				Z+=1
			if C==6:
				break
		RS.rewind()
		print "stats:  Zeros skipped: "+str(Z)+" Insufficient data to count: "+str(EJECTIONS)
		print "---------------------------------"
	print "Skipped "+str(SKIPS)+" records"
	return
def main_ProcInput():
	ALLWEIGHTS=getAllWeights()
	MASTERTITLE=raw_input("Please enter title of master page...")
	MASTERREC=retMD5(MASTERTITLE)
	VLIST,Misses=calcRatios(MASTERREC)
	print "Now enter pages, either matching or nonmatching but not both..."
	RECLIST=[]
 	while True:
		CLITITLE=raw_input("Enter child page name(or 'x' to quit):")
		if CLITITLE=='x':
			break
		CLIID=retMD5(CLITITLE)
	 	CLIREC={'_id':CLIID,'title':CLITITLE}
		RECLIST.append(CLIREC)
	RESCHOICE=raw_input("Done entering.  Are these pages matches(y) or not(n)?")
	print "Great! now processing ..."
	RL=[]
	for rec in RECLIST:
		TOTALSCORE,WeightsRecord,ScoreRecord,Misses=outputFunction(rec,VLIST,ALLWEIGHTS)
		insme=(TOTALSCORE,rec['_id'],rec['title'])
		RL.append(insme)
		bpnnRescoreWeights(str(RESCHOICE),WeightsRecord,ScoreRecord)
	if RESCHOICE=='n' or RESCHOICE=='N':
		nndb['nncounts'].update({'_id':'WikiNN'},{'$inc':{'NO':len(RECLIST)}},upsert=True)
	elif RESCHOICE=='y' or RESCHOICE=='Y':
		nndb['nncounts'].update({'_id':'WikiNN'},{'$inc':{'YES':len(RECLIST)}},upsert=True)
	print 'All done!'
	
				
def main_TestRandomPages():
	RS=db['en_hitsdaily'].find({'2013_10_07':{'$gt':500}})
	CHECKS=db['en_hitsdaily'].find({'2013_10_07':{'$gt':500}})
	for c in range(0,25):
		YESS=0
		NOS=0
		ALLWEIGHTS=getAllWeights()
		SEED=random.randint(0,CHECKS.count())
		MASTERREC=CHECKS[SEED]['_id'] 
		VLIST,Misses=calcRatios(MASTERREC)
		MASTERTITLE=CHECKS[SEED]['title']
		RL=[]
		print "Looking for similars to : "+str(MASTERTITLE)
		for clirec in RS:
			TOTALSCORE,WeightsRecord,ScoreRecord,Misses=outputFunction(clirec,VLIST,ALLWEIGHTS)
			insme=(TOTALSCORE,clirec['_id'],clirec['title'])
			RL.append(insme)
		print 'now sorting list of length '+str(len(RL))	
		SL=sorting.QuickSort(RL)
		#SL=sorted(RL)
		C=0
		for a in SL:
			if a[0]< 1 :
				continue
			print str(a[0])+" "+str(a[2])+"   \r"
			uinput=raw_input("Master: "+str(MASTERTITLE)+". Is this a match?(Y/N)")
			if uinput=='x':
				break
			if uinput=='c':
				continue
			if uinput=='y':
				YESS+=1
			if uinput=='n':
				NOS+=1
			bpnnRescoreWeights(str(uinput),WeightsRecord,ScoreRecord)
		nndb['nncounts'].update({'_id':'WikiNN'},{'$inc':{'YES':YESS}},upsert=True)
		nndb['nncounts'].update({'_id':'WikiNN'},{'$inc':{'NO':NOS}},upsert=True)
		RS.rewind()
	return

def main_TestSinglePage():
	LOWERBOUND=0.402
	UPPERBOUND=0.4462
	RS=db['en_hitsdaily'].find({'2013_10_07':{'$gt':500}})
	MASTERREC=hashlib.sha1('Breaking_Bad').hexdigest() 
	VLIST,Misses=calcRatios(MASTERREC)
	MASTERTITLE='Breaking Bad'
	ALLWEIGHTS=getAllWeights()
	RL=[]
	print "Looking for similars to : "+str(MASTERTITLE)
	for clirec in RS:
		TOTALSCORE,WeightsRecord,ScoreRecord,Misses=outputFunction(clirec,VLIST,ALLWEIGHTS)
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
		bpnnRescoreWeights(str(uinput),WeightsRecord,ScoreRecord)
	return

def main_SingleOFILE():
	HASHSTRING='Green_Bay_Packers'
	RS=db['en_hitsdaily'].find({'2013_10_07':{'$gt':500}})
	MASTERREC=hashlib.sha1(HASHSTRING).hexdigest()
	VLIST,Misses=calcRatios(MASTERREC)
	ALLWEIGHTS=getAllWeights()
	RL=[]
	print "Scoring for similars to "+str(HASHSTRING)
	for clirec in RS:
		TOTALSCORE,WeightsRecord,ScoreRecord,Misses=outputFunction(clirec,VLIST,ALLWEIGHTS)
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
#main_TestRandomPages()
#main_ProcInput()
main_InsertIntoDB()
