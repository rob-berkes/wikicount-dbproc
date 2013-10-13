from pymongo import Connection
from lib import sorting
import math
import sys
import random
import hashlib
import types

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
def nHourEval(cHOURLIST,mHOURLIST,HOURWEIGHTS):
	SCORELIST=list()
	MISSES=0
	for a in range(0,24):
		HOUR="%02d"%(a,)
		HOURSCORE=float(mHOURLIST[a])-float(cHOURLIST[a])
		try:
			HOURSCORE=(1/HOURSCORE)*(HOURWEIGHTS[a])
		except ZeroDivisionError:
			HOURSCORE=1
		SCORELIST.append(HOURSCORE)
	return SCORELIST
def nHourMinus1Eval(cHOURLIST,mHOURLIST,HOURM1WEIGHTS):
	SCORELIST=list()
	MISSES=0
	for a in range(0,24):
		if a==0:
			b=23
		else:
			b=a-1
		HOUR="%02d"%(a,)
		LASTHOUR="%02d"%(b,)
		HOURSCORE=float(mHOURLIST[a])-float(cHOURLIST[b])
		try:
			HOURSCORE=(1/HOURSCORE)*(HOURM1WEIGHTS[a])
		except ZeroDivisionError:
			HOURSCORE=1
		SCORELIST.append(HOURSCORE)
	return SCORELIST

def nTodayEval(cTR,mTR):
	return mTR-cTR
def nDayEval(D1RAT,mRAT):
	return mRAT-D1RAT



def scoreHourly(SCORELIST):
	score=0
	for a in range(0,24):
		if a==12:
			continue
		score+=math.fabs(SCORELIST[a])
	return score	
def retMD5(name):
	return hashlib.sha1(name).hexdigest()
 
def setWeight(RLIST,type):
	if type=='hourly':
		nndb['settings'].update({'_id':'WikiHourlyWeights'},{'$set':{'Values':RLIST}},upsert=True)
	elif type=='hourm1':
		nndb['settings'].update({'_id':'WikiHourM1Weights'},{'$set':{'Values':RLIST}},upsert=True)
	return

def findLowHour(REC,fromLowest):
	LOWEST=9999999
	LOWHOUR=0
	for a in range(0,24):
		try:
			if REC['HOURLY'][a]<LOWEST and REC['HOURLY'][a]!=0 and REC['HOURLY'][a]!=1:
				LOWEST=REC['HOURLY'][a]
				LOWHOUR=a
		except IndexError:
			continue
		
	return LOWHOUR
def findLowHM1(REC,fromLowest):
	LOWEST=9999999
	LOWHOUR=0
	for a in range(0,24):
		try:
			if REC['HM1'][a]<LOWEST and REC['HM1'][a]!=0 and REC['HM1'][a]!=1:
				LOWEST=REC['HM1'][a]
				LOWHOUR=a
		except IndexError:
			continue
		
	return LOWHOUR

def findHighHour(REC,fromHighest):
	HIGHEST=-1
	HIGHHOUR=99
	for a in range(0,24):
		if REC['HOURLY'][a]>HIGHEST and REC['HOURLY'][a]!=0 and REC['HOURLY'][a]!=1:
			HIGHEST=REC['HOURLY'][a]
			HIGHHOUR=a
	return HIGHHOUR
def findHighHM1(REC,fromHighest):
	HIGHEST=-1
	HIGHHOUR=99
	for a in range(0,24):
		if REC['HM1'][a]>HIGHEST and REC['HM1'][a]!=0 and REC['HM1'][a]!=1:
			HIGHEST=REC['HM1'][a]
			HIGHHOUR=a
	return HIGHHOUR
def bpnnRescoreWeights(RESULT,DAYWEIGHTS,HOURWEIGHTS,HOURM1WEIGHTS,ScoreRecord):
	HITVAL=0.005
	MISSVAL=-0.00001
	if RESULT=='N' or RESULT=='n':
		LOWHOUR=findLowHour(ScoreRecord,1)
		LOWHM1=findLowHM1(ScoreRecord,1)
		NewWeight=float(HOURWEIGHTS[LOWHOUR])+float(MISSVAL)
		NewHM1W=float(HOURM1WEIGHTS[LOWHM1])+float(MISSVAL)
		HOURWEIGHTS[LOWHOUR]=float(NewWeight)
		HOURM1WEIGHTS[LOWHM1]=float(NewHM1W)
		setWeight(HOURWEIGHTS,'hourly')
		setWeight(HOURM1WEIGHTS,'hourm1')
	elif RESULT=='Y' or RESULT=='y':
		HIGHHOUR=findHighHour(ScoreRecord,1)
		HIGHHM1=findHighHM1(ScoreRecord,1)
		NewWeight=float(HOURWEIGHTS[HIGHHOUR])+float(HITVAL)
		NewHM1W=float(HOURM1WEIGHTS[HIGHHM1])+float(HITVAL)
		HOURWEIGHTS[HIGHHOUR]=float(NewWeight)
		HOURM1WEIGHTS[HIGHHM1]=float(NewHM1W)
		setWeight(HOURWEIGHTS,'hourly')
		setWeight(HOURM1WEIGHTS,'hourm1')
	# if neither y or n, do nothing
	return
def testRatioAllHours(IDREC):
	#Compares one hour to it's next 
	rsD=db['en_hitshourly'].find_one({'_id':IDREC})
	MISSES=0
	HOURLIST=list()
	for a in range(0,24):
		if a==23:
			b=0
		else:
			b=a+1
		HOURNAME="%02d" % (a,)
		NEXTHOURNAME="%02d" % (b,)
		try:
			HOURRATIO=float(rsD[HOURNAME])/rsD[NEXTHOURNAME]
		except:
			MISSES+=1
			HOURRATIO=1
		HOURLIST.append(HOURRATIO)
	return HOURLIST,MISSES
def calcRatios(MASTERREC):
	#Save time by doing these calcs only once
	Misses=0
	d5RAT=1
	d6RAT=1
	d4RAT=1.0
	d3RAT=1.0
	d2RAT=1.0
	d1RAT=1.0
	mTR=1.0
	
	HOURLIST,HOURMISSES=testRatioAllHours(MASTERREC)

	VLIST=(mTR,d1RAT,d2RAT,d3RAT,d4RAT,d5RAT,d6RAT)
	return VLIST,HOURLIST,Misses,HOURMISSES

def getAllWeights():
	DAYWEIGHTS=nndb['settings'].find_one({'_id':'WikiDayWeights'})
	try:
		DAYWEIGHTS=DAYWEIGHTS['Values']
	except TypeError:
		DAYWEIGHTS=[]
		for a in range(0,367):
			DAYWEIGHTS.append(float(1))
		
	HOURWEIGHTS=nndb['settings'].find_one({'_id':'WikiHourlyWeights'})
	try:
		HOURWEIGHTS=HOURWEIGHTS['Values']
	except TypeError:
		HOURWEIGHTS=[]
		for a in range(0,24):
			HOURWEIGHTS.append(float(1))
	HOURM1WEIGHTS=nndb['settings'].find_one({'_id':'WikiHourM1Weights'})
	try:
		HOURM1WEIGHTS=HOURM1WEIGHTS['Values']
	except TypeError:
		HOURM1WEIGHTS=[]
		for a in range(0,24):
			HOURM1WEIGHTS.append(float(1))
	return DAYWEIGHTS,HOURWEIGHTS,HOURM1WEIGHTS
	
def clientOutputFunction(clirec,MASTERLIST,MASTERHOURLIST,DAYWEIGHTS,HOURWEIGHTS,HOURM1WEIGHTS):
	D1Weight=DAYWEIGHTS[1]
	D2Weight=DAYWEIGHTS[2]
	try:
		D3Weight=DAYWEIGHTS[3]
	except:
		D3Weight=1
	try:
		D4Weight=DAYWEIGHTS[4]
	except:
		D4Weight=1
	try:
		D5Weight=DAYWEIGHTS[5]
	except:
		D5Weight=1
	try:
		D6Weight=ALLWEIGHTS[6]
	except:
		D6Weight=1
	try:
		T1SWeight=HOURM1WEIGHTS[1]
	except:
		T1SWeight=1
	CLIST,HOURLIST,Misses,HOURMISSES=calcRatios(clirec['_id'])
	SCORELIST=nHourEval(HOURLIST,MASTERHOURLIST,HOURWEIGHTS)
	HM1SCORELIST=nHourMinus1Eval(HOURLIST,MASTERHOURLIST,HOURM1WEIGHTS)
	OUTPUT=scoreHourly(SCORELIST)
	OUTPUT+=scoreHourly(HM1SCORELIST)
#	try:
#		T1S=(1/nTodayEval(CLIST[0],MASTERLIST[0]))*T1SWeight
#	except ZeroDivisionError:
#		T1S=1
#	try:
#		D1=(1/nDayEval(CLIST[1],MASTERLIST[1]))*D1Weight
#	except ZeroDivisionError:
#		D1=1
#
#	try:
#		D2=(1/nDayEval(CLIST[2],MASTERLIST[2]))*D2Weight
#	except ZeroDivisionError:
#		D2=1
#
#	try:
#		D3=(1/nDayEval(CLIST[3],MASTERLIST[3]))*D3Weight
#	except ZeroDivisionError:
#		D3=1
#	try:
#		D4=(1/nDayEval(CLIST[4],MASTERLIST[4]))*D4Weight
#	except ZeroDivisionError:
#		D4=1
#
#	try:
#		D5=(1/nDayEval(CLIST[5],MASTERLIST[5]))*D5Weight
#	except ZeroDivisionError:
#		D5=1
#	try:
#		D6=(1/nDayEval(CLIST[6],MASTERLIST[6]))*D6Weight
#	except ZeroDivisionError:
#		D6=1
#	print H1S,H2S,T1S,D1,D2,D3
#	OUTPUT+=math.fabs(T1S)+math.fabs(D1)+math.fabs(D2)+math.fabs(D3)+math.fabs(D4)+math.fabs(D5)+math.fabs(D6)
#	ScoreRecord={'T1S':T1S,'D1':D1,'D2':D2,'D3':D3,'D4':D4,'D5':D5,'D6':D6,'HOURLY':HOURLIST}
	ScoreRecord={'HOURLY':SCORELIST,'HM1':HM1SCORELIST}
	return  OUTPUT,ScoreRecord,Misses,DAYWEIGHTS,HOURWEIGHTS,HOURM1WEIGHTS

def main_InsertIntoDB():
	MRS=db['en_threehour'].find()
	RS=db['en_hitsdaily'].find({'2013_10_10':{'$gt':500}})
	DAYWEIGHTS,HOURWEIGHTS,HOURM1WEIGHTS=getAllWeights()
	SKIPS=0
	for mrec in MRS:
		EJECTIONS=0
		MASTERREC=mrec['id']
		VLIST,HOURLIST,MMisses,HOURMISSES=calcRatios(MASTERREC)
		if MMisses>4:
			SKIPS+=1
			continue
		MASTERTITLE=mrec['title']
		RL=[]
		EJECTIONS=0
		print '[main_InsertIntoDB] Producing similars for '+str(MASTERTITLE)
		for clirec in RS:
			TOTALSCORE,ScoreRecord,Misses,DAYWEIGHTS,HOURWEIGHTS,HOURM1WEIGHTS=clientOutputFunction(clirec,VLIST,HOURLIST,DAYWEIGHTS,HOURWEIGHTS,HOURM1WEIGHTS)
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
			if s[-COUNT]!=0:
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
	DAYWEIGHTS,HOURWEIGHTS,HOURM1WEIGHTS=getAllWeights()
	SKIPS=0
	for mrec in MRS:
		MASTERREC=mrec['id']
		VLIST,HOURLIST,MMisses,HOURMISSES=calcRatios(MASTERREC)
		print 'master '+str(HOURMISSES)+' Hour misses and '+str(MMisses)+' other misses'
		if MMisses>4:
			print 'skipping, '+str(MMisses)+' misses!'
			SKIPS+=1
			continue
		MASTERTITLE=mrec['title']
		RL=[]
		EJECTIONS=0
		print '[main_Production] Producing similars for '+str(MASTERTITLE)
		for clirec in RS:
			TOTALSCORE,ScoreRecord,Misses,DAYWEIGHTS,HOURWEIGHTS,HOURM1WEIGHTS=clientOutputFunction(clirec,VLIST,HOURLIST,DAYWEIGHTS,HOURWEIGHTS,HOURM1WEIGHTS)
			if Misses<20:
				insme=(TOTALSCORE,clirec['_id'],clirec['title'])
				RL.append(insme)
			else:
				EJECTIONS+=1
		print '[main_Production] now sorting list of '+str(len(RL))
		SL=sorting.QuickSort(RL)
		C=0
		Z=0
		for x in range(0,25):
			if SL[-C][0]!=0:
				print SL[-C]
				C+=1
			else:
				Z+=1
			if C>6:
				break
		RS.rewind()
		print "stats:  Zeros skipped: "+str(Z)+" Insufficient data to count: "+str(EJECTIONS)
		print "---------------------------------"
	print "Skipped "+str(SKIPS)+" records"
	return
def main_ProcInput():
	DAYWEIGHTS,HOURWEIGHTS,HOURM1WEIGHTS=getAllWeights()
	MASTERTITLE=raw_input("Please enter title of master page...")
	MASTERREC=retMD5(MASTERTITLE)
	VLIST,HOURLIST,Misses,HOURMISSES=calcRatios(MASTERREC)
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
		TOTALSCORE,ScoreRecord,Misses,DAYWEIGHTS,HOURWEIGHTS,HOURM1WEIGHTS=clientOutputFunction(rec,VLIST,HOURLIST,DAYWEIGHTS,HOURWEIGHTS,HOURM1WEIGHTS)
		insme=(TOTALSCORE,rec['_id'],rec['title'])
		RL.append(insme)
		bpnnRescoreWeights(str(RESCHOICE),DAYWEIGHTS,HOURWEIGHTS,HOURM1WEIGHTS,ScoreRecord)
	if RESCHOICE=='n' or RESCHOICE=='N':
		nndb['nncounts'].update({'_id':'WikiNN'},{'$inc':{'NO':len(RECLIST)}},upsert=True)
	elif RESCHOICE=='y' or RESCHOICE=='Y':
		nndb['nncounts'].update({'_id':'WikiNN'},{'$inc':{'YES':len(RECLIST)}},upsert=True)
	print 'All done!'
	
				
def main_TestRandomPages():
	RS=db['en_hitsdaily'].find({'2013_10_10':{'$gt':200}})
	CHECKS=db['en_hitsdaily'].find({'2013_10_10':{'$gt':500}})
	DAYWEIGHTS,HOURWEIGHTS,HOURM1WEIGHTS=getAllWeights()
	for c in range(0,25):
		YESS=0
		NOS=0
		SEED=random.randint(0,CHECKS.count())
		MASTERREC=CHECKS[SEED]['_id'] 
		VLIST,MASTERHOURLIST,Misses,HOURMISSES=calcRatios(MASTERREC)
		MASTERTITLE=CHECKS[SEED]['title']
		RL=[]
		print "Looking for similars to : "+str(MASTERTITLE)
		rsCOUNT=0
		for clirec in RS:
			TOTALSCORE,ScoreRecord,Misses,DAYWEIGHTS,HOURWEIGHTS,HOURM1WEIGHTS=clientOutputFunction(clirec,VLIST,MASTERHOURLIST,DAYWEIGHTS,HOURWEIGHTS,HOURM1WEIGHTS)
			insme=(TOTALSCORE,clirec['_id'],clirec['title'])
			RL.append(insme)
			rsCOUNT+=1
			if rsCOUNT % 10000==0:
				print "[main_testRandomPages] Rec count "+str(rsCOUNT)+" reached. "+str(RS.count())+" total."
		print 'now sorting list of length '+str(len(RL))	
		SL=sorting.QuickSort(RL)
		#SL=sorted(RL)
		C=0
		for c in range(1,100):
			if SL[-c][0]< 1 :
				continue
			print str(SL[-c][0])+" "+str(SL[-c][2])+"   \r"
			uinput=raw_input("Master: "+str(MASTERTITLE)+". Is this a match?(Y/N)")
			if uinput=='x':
				break
			if uinput=='c':
				continue
			if uinput=='y':
				YESS+=1
			if uinput=='n':
				NOS+=1
			bpnnRescoreWeights(str(uinput),DAYWEIGHTS,MASTERHOURLIST,HOURM1WEIGHTS,ScoreRecord)
		nndb['nncounts'].update({'_id':'WikiNN'},{'$inc':{'YES':YESS}},upsert=True)
		nndb['nncounts'].update({'_id':'WikiNN'},{'$inc':{'NO':NOS}},upsert=True)
		RS.rewind()
	return

def main_TestSinglePage():
	MASTERTITLE='Tetraphobia'
	MAXCOUNT=25
	RS=db['en_hitsdaily'].find({'2013_10_07':{'$gt':500}})
	MASTERREC=hashlib.sha1(MASTERTITLE).hexdigest() 
	DAYWEIGHTS,HOURWEIGHTS,HOURM1WEIGHTS=getAllWeights()
	VLIST,HOURLIST,Misses,HOURMISSES=calcRatios(MASTERREC)
	RL=[]
	print "Looking for similars to : "+str(MASTERTITLE)
	for clirec in RS:
		TOTALSCORE,ScoreRecord,Misses=clientOutputFunction(clirec,VLIST,HOURLIST,DAYWEIGHTS,HOURWEIGHTS,HOURM1WEIGHTS)
		insme=(TOTALSCORE,clirec['_id'],clirec['title'])
		RL.append(insme)
	print 'now sorting list of length '+str(len(RL))	
	SL=sorting.QuickSort(RL)
	#SL=sorted(RL)
	C=0
	for a in SL:
		if C>MAXCOUNT:
			break
		else:
			C+=1
		print str(a)+'\r'
		uinput=raw_input("Master: "+str(MASTERTITLE)+". Is this a match?(Y/N)")
		if str(uinput)=='x':
			break
		bpnnRescoreWeights(str(uinput),DAYWEIGHTS,HOURWEIGHTS,HOURM1WEIGHTS,ScoreRecord)
	return

def main_SingleOFILE():
	HASHSTRING='Green_Bay_Packers'
	RS=db['en_hitsdaily'].find({'2013_10_07':{'$gt':500}})
	MASTERREC=hashlib.sha1(HASHSTRING).hexdigest()
	VLIST,HOURLIST,Misses,HOURMISSES=calcRatios(MASTERREC)
	DAYWEIGHTS,HOURWEIGHTS,HOURM1WEIGHTS=getAllWeights()
	RL=[]
	print "Scoring for similars to "+str(HASHSTRING)
	for clirec in RS:
		TOTALSCORE,ScoreRecord,Misses=clientOutputFunction(clirec,VLIST,HOURLIST,DAYWEIGHTS,HOURWEIGHTS,HOURM1WEIGHTS)
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
#main_ProcInput()
#main_InsertIntoDB()
