from pymongo import Connection
from lib import sorting
from datetime import date
import math
import sys
import random
import hashlib
import types

conn=Connection()
db=conn.wc
nndb=conn.neuralweights
def nHourEval(cHOURRATIOS,mHOURRATIOS,HOURWEIGHTS):
	SCORELIST=list()
	MISSES=0
	for a in range(0,24):
		HOUR="%02d"%(a,)
		:HOURSCORE=float(mHOURRATIOS[a])-float(cHOURRATIOS[a])
		try:
			HOURSCORE=(1/HOURSCORE)*(HOURWEIGHTS[a])
		except ZeroDivisionError:
			HOURSCORE=1
		SCORELIST.append(HOURSCORE)
	return SCORELIST
def nHourMinus1Eval(cHOURRATIOS,mHOURRATIOS,HOURM1WEIGHTS):
	SCORELIST=list()
	MISSES=0
	for a in range(0,24):
		if a==0:
			b=23
		else:
			b=a-1
		HOUR="%02d"%(a,)
		LASTHOUR="%02d"%(b,)
		HOURSCORE=float(mHOURRATIOS[a])-float(cHOURRATIOS[b])
		try:
			HOURSCORE=(1/HOURSCORE)*(HOURM1WEIGHTS[a])
		except ZeroDivisionError:
			HOURSCORE=1
		SCORELIST.append(HOURSCORE)
	return SCORELIST

def nTodayEval(cTR,mTR):
	return mTR-cTR
def nDayEval(DAYRATIOS,MASTERDAYRATIOS,DAYWEIGHTS):
	SCORELIST=list()
	NOW=date.today()
	DAY=NOW.day
	MISSES=0
	for a in range(1,DAY):
		try:
			DAYSCORE=float(MASTERDAYRATIOS[a])-float(DAYRATIOS[a])
		except IndexError:
			DAYSCORE=1.0
			SCORELIST.append(DAYSCORE)
			break
		try:
			DAYSCORE=float(1/DAYSCORE)*DAYWEIGHTS[a]
		except ZeroDivisionError:
			DAYSCORE=1.0
		SCORELIST.append(DAYSCORE)
	return SCORELIST


def scoreDaily(SCORELIST):
	NOW=date.today()
	DAY=NOW.day
	SCORE=0
	for a in range(1,DAY):
		try:
			SCORE+=math.fabs(SCORELIST[a])
		except:
			break
	return SCORE

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
	elif type=='daily':
		nndb['settings'].update({'_id':'WikiDayWeights'},{'$set':{'Values':RLIST}},upsert=True)
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
	HIGHHOUR=0
	for a in range(0,24):
		try:
			if REC['HOURLY'][a]>HIGHEST and REC['HOURLY'][a]!=0 and REC['HOURLY'][a]!=1:
				HIGHEST=REC['HOURLY'][a]
				HIGHHOUR=a
		except IndexError:
			break
	return HIGHHOUR
def findHighHM1(REC,fromHighest):
	HIGHEST=-1
	HIGHHOUR=99
	for a in range(0,24):
		if REC['HM1'][a]>HIGHEST and REC['HM1'][a]!=0 and REC['HM1'][a]!=1:
			HIGHEST=REC['HM1'][a]
			HIGHHOUR=a
	return HIGHHOUR
def findHighDay(REC,fromHighest):
	HIGHEST=-1
	HIGHDAY=0
	for a in range(1,15):
		try:
			if REC['DAY'][a]>HIGHEST and REC['DAY'][a]!=0 and REC['DAY'][a]!=1:
				HIGHEST=REC['DAY'][a]
				HIGHDAY=a
		except:
			break
	return HIGHDAY
def bpnnRescoreWeights(RESULT,DAYWEIGHTS,HOURWEIGHTS,HOURM1WEIGHTS,ScoreRecord):
	HITVAL=0.005
	MISSVAL=-0.00001
	if RESULT=='N' or RESULT=='n':
		HIGHHOUR=findHighHour(ScoreRecord,1)
		HIGHHM1=findHighHM1(ScoreRecord,1)
		HIGHDAY=findHighDay(ScoreRecord,1)
		NewDayWeight=float(DAYWEIGHTS[HIGHDAY])+MISSVAL
		NewWeight=float(HOURWEIGHTS[HIGHHOUR])+MISSVAL
		NewHM1W=float(HOURM1WEIGHTS[HIGHHM1])+MISSVAL
		HOURWEIGHTS[HIGHHOUR]=float(NewWeight)
		HOURM1WEIGHTS[HIGHHM1]=float(NewHM1W)
		DAYWEIGHTS[HIGHDAY]=NewDayWeight
		setWeight(DAYWEIGHTS,'daily')
		setWeight(HOURWEIGHTS,'hourly')
		setWeight(HOURM1WEIGHTS,'hourm1')
	elif RESULT=='Y' or RESULT=='y':
		HIGHDAY=findHighDay(ScoreRecord,1)
		NewDayWeight=float(DAYWEIGHTS[HIGHDAY])+HITVAL
		HIGHHOUR=findHighHour(ScoreRecord,1)
		HIGHHM1=findHighHM1(ScoreRecord,1)
		NewWeight=float(HOURWEIGHTS[HIGHHOUR])+float(HITVAL)
		NewHM1W=float(HOURM1WEIGHTS[HIGHHM1])+float(HITVAL)
		HOURWEIGHTS[HIGHHOUR]=float(NewWeight)
		HOURM1WEIGHTS[HIGHHM1]=float(NewHM1W)
		DAYWEIGHTS[HIGHDAY]=NewDayWeight
		setWeight(DAYWEIGHTS,'daily')
		setWeight(HOURWEIGHTS,'hourly')
		setWeight(HOURM1WEIGHTS,'hourm1')
	# if neither y or n, do nothing
	return
def testRatioAllHours(IDREC):
	#Compares one hour to it's next 
	rsD=db['en_hitshourly'].find_one({'_id':IDREC})
	MISSES=0
	HOURRATIOS=list()
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
		HOURRATIOS.append(HOURRATIO)
	return HOURRATIOS,MISSES
def ratioDaysCurMonth(IDREC):
	#ratio hits compared to next day
	rsD=db['en_hitsdaily'].find_one({'_id':IDREC})
	MISSES=0
	NOW=date.today()
	MONTH=NOW.month
	MONTHSTR="%02d" % (MONTH,)
	DAY=NOW.day
	DAYRATIOS=list()
	for a in range(1,DAY):
		NEXTDAY=a+1
		DAYSR="%02d" %(a,)
		NDAYSR="%02d" % (NEXTDAY,)
		NDAYSTR="2013_"+MONTHSTR+"_"+NDAYSR
		DAYSTR="2013_"+MONTHSTR+"_"+DAYSR
		try:
			DAYRATIO=float(rsD[DAYSTR])/rsD[NDAYSTR]
		except:
			MISSES+=1
			DAYRATIO=1
		DAYRATIOS.append(DAYRATIO)
	return DAYRATIOS,MISSES
def calcRatios(MASTERREC):
	#Save time by doing these calcs only once
	
	HOURRATIOS,HOURMISSES=testRatioAllHours(MASTERREC)
	DAYRATIOS,DAYMISSES=ratioDaysCurMonth(MASTERREC)

	return DAYRATIOS,HOURRATIOS,DAYMISSES,HOURMISSES

def getAllWeights():
	DAYWEIGHTS=nndb['settings'].find_one({'_id':'WikiDayWeights'})
	try:
		DAYWEIGHTS=DAYWEIGHTS['Values']
	except TypeError:
		DAYWEIGHTS=[]
		for a in range(0,20):
			DAYWEIGHTS.append(random.uniform(0.95,0.98))
	HOURWEIGHTS=nndb['settings'].find_one({'_id':'WikiHourlyWeights'})
	try:
		HOURWEIGHTS=HOURWEIGHTS['Values']
	except TypeError:
		HOURWEIGHTS=[]
		for a in range(0,24):
			HOURWEIGHTS.append(random.uniform(0.95,0.98))
	HOURM1WEIGHTS=nndb['settings'].find_one({'_id':'WikiHourM1Weights'})
	try:
		HOURM1WEIGHTS=HOURM1WEIGHTS['Values']
	except TypeError:
		HOURM1WEIGHTS=[]
		for a in range(0,24):
			HOURM1WEIGHTS.append(random.uniform(0.95,0.98))
	return DAYWEIGHTS,HOURWEIGHTS,HOURM1WEIGHTS
	
def clientOutputFunction(clirec,MASTERDAYRATIOS,MASTERHOURRATIOS,DAYWEIGHTS,HOURWEIGHTS,HOURM1WEIGHTS):
	DAYRATIOS,HOURRATIOS,DAYMISSES,HOURMISSES=calcRatios(clirec['_id'])
	SCORELIST=nHourEval(HOURRATIOS,MASTERHOURRATIOS,HOURWEIGHTS)
	HM1SCORELIST=nHourMinus1Eval(HOURRATIOS,MASTERHOURRATIOS,HOURM1WEIGHTS)
	DAYSCORELIST=nDayEval(DAYRATIOS,MASTERDAYRATIOS,DAYWEIGHTS)

	OUTPUT=scoreDaily(DAYSCORELIST)
	OUTPUT+=scoreHourly(SCORELIST)
	OUTPUT+=scoreHourly(HM1SCORELIST)
 	ScoreRecord={'HOURLY':SCORELIST,'HM1':HM1SCORELIST,'DAY':DAYSCORELIST}
	Misses=DAYMISSES+HOURMISSES
	return  OUTPUT,ScoreRecord,Misses,DAYWEIGHTS,HOURWEIGHTS,HOURM1WEIGHTS

def main_InsertIntoDB():
	MRS=db['en_threehour'].find()
	RS=db['en_hitsdaily'].find({'2013_10_13':{'$gt':500}})
	DAYWEIGHTS,HOURWEIGHTS,HOURM1WEIGHTS=getAllWeights()
	SKIPS=0
	for mrec in MRS:
		EJECTIONS=0
		MASTERREC=mrec['id']
		DAYRATIOS,HOURRATIOS,MMisses,HOURMISSES=calcRatios(MASTERREC)
		if MMisses>4:
			SKIPS+=1
			continue
		MASTERTITLE=mrec['title']
		RL=[]
		EJECTIONS=0
		print '[main_InsertIntoDB] Producing similars for '+str(MASTERTITLE)
		for clirec in RS:
			TOTALSCORE,ScoreRecord,Misses,DAYWEIGHTS,HOURWEIGHTS,HOURM1WEIGHTS=clientOutputFunction(clirec,DAYRATIOS,HOURRATIOS,DAYWEIGHTS,HOURWEIGHTS,HOURM1WEIGHTS)
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
	RS=db['en_hitsdaily'].find({'2013_10_13':{'$gt':500}})
	DAYWEIGHTS,HOURWEIGHTS,HOURM1WEIGHTS=getAllWeights()
	SKIPS=0
	for mrec in MRS:
		MASTERREC=mrec['id']
		DAYRATIOS,HOURRATIOS,MMisses,HOURMISSES=calcRatios(MASTERREC)
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
			TOTALSCORE,ScoreRecord,Misses,DAYWEIGHTS,HOURWEIGHTS,HOURM1WEIGHTS=clientOutputFunction(clirec,DAYRATIOS,HOURRATIOS,DAYWEIGHTS,HOURWEIGHTS,HOURM1WEIGHTS)
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
	DAYRATIOS,HOURRATIOS,Misses,HOURMISSES=calcRatios(MASTERREC)
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
		TOTALSCORE,ScoreRecord,Misses,DAYWEIGHTS,HOURWEIGHTS,HOURM1WEIGHTS=clientOutputFunction(rec,DAYRATIOS,HOURRATIOS,DAYWEIGHTS,HOURWEIGHTS,HOURM1WEIGHTS)
		insme=(TOTALSCORE,rec['_id'],rec['title'])
		RL.append(insme)
		bpnnRescoreWeights(str(RESCHOICE),DAYWEIGHTS,HOURWEIGHTS,HOURM1WEIGHTS,ScoreRecord)
	if RESCHOICE=='n' or RESCHOICE=='N':
		nndb['nncounts'].update({'_id':'WikiNN'},{'$inc':{'NO':len(RECLIST)}},upsert=True)
	elif RESCHOICE=='y' or RESCHOICE=='Y':
		nndb['nncounts'].update({'_id':'WikiNN'},{'$inc':{'YES':len(RECLIST)}},upsert=True)
	print 'All done!'
	
				
def main_TestRandomPages():
	RS=db['en_hitsdaily'].find({'2013_10_13':{'$gt':1500}})
	CHECKS=db['en_hitsdaily'].find({'2013_10_13':{'$gt':1500}})
	DAYWEIGHTS,HOURWEIGHTS,HOURM1WEIGHTS=getAllWeights()
	for c in range(0,25):
		YESS=0
		NOS=0
		SEED=random.randint(0,CHECKS.count())
		MASTERREC=CHECKS[SEED]['_id'] 
		DAYRATIOS,MASTERHOURRATIOS,Misses,HOURMISSES=calcRatios(MASTERREC)
		MASTERTITLE=CHECKS[SEED]['title']
		RL=[]
		print "Looking for similars to : "+str(MASTERTITLE)
		rsCOUNT=0
		for clirec in RS:
			TOTALSCORE,ScoreRecord,Misses,DAYWEIGHTS,HOURWEIGHTS,HOURM1WEIGHTS=clientOutputFunction(clirec,DAYRATIOS,MASTERHOURRATIOS,DAYWEIGHTS,HOURWEIGHTS,HOURM1WEIGHTS)
			insme=(TOTALSCORE,clirec['_id'],clirec['title'])
			RL.append(insme)
			rsCOUNT+=1
			if rsCOUNT % 10000==0:
				print "[main_testRandomPages] Rec count "+str(rsCOUNT)+" reached. "+str(RS.count())+" total."
		print 'now sorting list of length '+str(len(RL))	
		SL=sorting.QuickSort(RL)
		#SL=sorted(RL)
		for c in range(1,30):
			uinput=raw_input("("+str(c)+")_Master: "+str(MASTERTITLE)+". Score "+str(SL[-c][0])+" for RECORD "+str(SL[-c][2])+". Is this a match?(Y/N)")
			vinput=raw_input("("+str(c)+")_Master: "+str(MASTERTITLE)+". Score "+str(SL[c][0])+" for RECORD "+str(SL[c][2])+". Is this a match?(Y/N)")
			if uinput=='x':
				break
			if uinput=='c':
				continue
			if uinput=='y':
				YESS+=1
			if uinput=='n':
				NOS+=1
			bpnnRescoreWeights(str(uinput),DAYWEIGHTS,MASTERHOURRATIOS,HOURM1WEIGHTS,ScoreRecord)
		nndb['nncounts'].update({'_id':'WikiNN'},{'$inc':{'YES':YESS}},upsert=True)
		nndb['nncounts'].update({'_id':'WikiNN'},{'$inc':{'NO':NOS}},upsert=True)
		RS.rewind()
	return

def main_TestSinglePage():
	MASTERTITLE='Star_Trek'
	MAXCOUNT=25
	RS=db['en_hitsdaily'].find({'2013_10_13':{'$gt':100}})
	MASTERREC=hashlib.sha1(MASTERTITLE).hexdigest() 
	DAYWEIGHTS,HOURWEIGHTS,HOURM1WEIGHTS=getAllWeights()
	DAYRATIOS,HOURRATIOS,Misses,HOURMISSES=calcRatios(MASTERREC)
	RL=[]
	print "Looking for similars to : "+str(MASTERTITLE)
	rc=0
	for clirec in RS:
		if rc % 10000 == 0:
			print "[mTSP] Scored "+str(rc)+" of "+str(RS.count())+" records."
			rc+=1
		else:
			rc+=1
		TOTALSCORE,ScoreRecord,Misses,DAYWEIGHTS,HOURWEIGHTS,HOURM1WEIGHTS=clientOutputFunction(clirec,DAYRATIOS,HOURRATIOS,DAYWEIGHTS,HOURWEIGHTS,HOURM1WEIGHTS)
		insme=(TOTALSCORE,clirec['_id'],clirec['title'])
		RL.append(insme)
	print 'now sorting list of length '+str(len(RL))	
	SL=sorting.QuickSort(RL)
	#SL=sorted(RL)
	C=1
	for C in range(0,222):
		if C>MAXCOUNT:
			break
		else:
			C+=1
		print str(SL[-C])+'\r'
		uinput=raw_input("Master: "+str(MASTERTITLE)+". Is this a match?(Y/N)")
		if str(uinput)=='x':
			break
		bpnnRescoreWeights(str(uinput),DAYWEIGHTS,HOURWEIGHTS,HOURM1WEIGHTS,ScoreRecord)
	return

def main_SingleOFILE():
	HASHSTRING='Green_Bay_Packers'
	RS=db['en_hitsdaily'].find({'2013_10_13':{'$gt':500}})
	MASTERREC=hashlib.sha1(HASHSTRING).hexdigest()
	DAYRATIOS,HOURRATIOS,Misses,HOURMISSES=calcRatios(MASTERREC)
	DAYWEIGHTS,HOURWEIGHTS,HOURM1WEIGHTS=getAllWeights()
	RL=[]
	print "Scoring for similars to "+str(HASHSTRING)
	for clirec in RS:
		TOTALSCORE,ScoreRecord,Misses=clientOutputFunction(clirec,DAYRATIOS,HOURRATIOS,DAYWEIGHTS,HOURWEIGHTS,HOURM1WEIGHTS)
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
