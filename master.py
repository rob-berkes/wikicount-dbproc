#!/usr/bin/python
#coding: utf-8
import urllib2
import os
import gzip 
from functions import wikicount
from time import time
import syslog
import glob
import re
import string
from multiprocessing import Process,Pipe,Queue
import random
from pymongo import Connection
from lib import sorting
import hashlib
from datetime import date
from numpy import sqrt,mean,array
from operator import itemgetter
import urllib
import httplib #used in checking for 404s 200s etc

FILEBASE="/tmp/staging/pagecounts.tmp"
DAY,MONTH,YEAR,HOUR,expiretime=wikicount.fnReturnTimes()
HOUR=wikicount.minusHour(int(HOUR))
HOUR=wikicount.minusHour(int(HOUR))
#HOUR=wikicount.minusHour(int(HOUR))
#HOUR=wikicount.minusHour(int(HOUR))
DAY,MONTH,HOUR=wikicount.fnFormatTimes(DAY,MONTH,HOUR)
LANGLIST=wikicount.getLanguageList()
LANGUAGES=wikicount.getLanguageList()
vTODAY=date.today()
vDOW=vTODAY.weekday()


import time
WEEKDAY=time.strftime("%w")
from time import time




def returnInvertedHour(HOUR):
	if int(HOUR) < 12: 
		return str(int(HOUR)+12)
	elif int(HOUR) > 11:
		return str(int(HOUR)-12)
def p0_dl():
	URL="http://dumps.wikimedia.org/other/pagecounts-raw/"+str(YEAR)+"/"+str(YEAR)+"-"+str(MONTH)+"/pagecounts-"
	URLDATE=str(YEAR)+str(MONTH)+str(DAY)
	URLSUFFIX="-"+str(HOUR)+"*.gz"
	URL+=URLDATE
	URL+=URLSUFFIX

	#Now with URL, download file
	a=time()
	syslog.syslog("[p0] - p0DL "+str(URL))
	missing=True
	MINUTESEARCH=0
	while missing and MINUTESEARCH<60:
		MINUTE=wikicount.fnFormatHour(MINUTESEARCH)
		URL="http://dumps.wikimedia.org/other/pagecounts-raw/"+str(YEAR)+"/"+str(YEAR)+"-"+str(MONTH)+"/pagecounts-"
		#print HOUR
		URLDATE=str(YEAR)+str(MONTH)+str(DAY)
		URLSUFFIX="-"+str(HOUR)+"00"+str(MINUTE)+".gz"
		URL+=URLDATE
		URL+=URLSUFFIX
		MINUTESEARCH+=1	
		try:	
			print URL
			COUNTFILE=urllib2.urlopen(URL)
			if COUNTFILE.code==200:
				fetchURL=URL
				print "fetchURL: "+str(fetchURL)
				missing=False
				syslog.syslog("[p0_dl] p0MINUTEFOUND "+str(fetchURL))
		except urllib2.HTTPError, e:
			print e.fp.read()
	
	if not missing:
		syslog.syslog("[p0_dl] p0DLSTART "+str(fetchURL))
		COUNTFILE=urllib2.urlopen(fetchURL)
		OFILE=open(FILEBASE,"w")
		OFILE.write(COUNTFILE.read())
		OFILE.close()
		COUNTFILE=urllib2.urlopen(fetchURL)
		CFILE=open("/root/pagecounts.tmp.gz","w")
	
		CFILE.write(COUNTFILE.read())
		CFILE.close()
		b=time()
		c=b-a
		d=round(c,3)
		syslog.syslog("[p0_dl] - p0200 "+str(URL)+" : "+str(d)+" seconds.")
	else:
		syslog.syslog("[p0_dl] - p0404, 404 not found error")



def p1_split():

	a=time()
	syslog.syslog("[p1_split] : begin")

	IFILE=gzip.open(FILEBASE,"r")
	
	NUMBERLOGFILES=4
	COUNTTHRESHOLD=2
	RECCOUNT=0

	for line in IFILE:
	        record=line.strip().split()
		record[0]=record[0].strip('.')
		RECCOUNT+=1
		try:
			if record[0] in LANGLIST and int(record[2])>COUNTTHRESHOLD and RECCOUNT % NUMBERLOGFILES==0:
				fName='/tmp/'+str(record[0])+'_staging/q1_pagecounts.'+str(HOUR)
				fFILE=open(fName,"a")
				fFILE.write(str(line))
				fFILE.close()
		        elif record[0] in LANGLIST and int(record[2])>COUNTTHRESHOLD and RECCOUNT % NUMBERLOGFILES == 1:
				fName='/tmp/'+str(record[0])+'_staging/q2_pagecounts.'+str(HOUR)
				fFILE=open(fName,"a")
				fFILE.write(str(line))
				fFILE.close()
		        elif record[0] in LANGLIST and int(record[2])>COUNTTHRESHOLD and RECCOUNT % NUMBERLOGFILES == 2:
				fName='/tmp/'+str(record[0])+'_staging/q3_pagecounts.'+str(HOUR)
				fFILE=open(fName,"a")
				fFILE.write(str(line))
				fFILE.close()
		        elif record[0] in LANGLIST and int(record[2])>COUNTTHRESHOLD and RECCOUNT % NUMBERLOGFILES == 3:
				fName='/tmp/'+str(record[0])+'_staging/q4_pagecounts.'+str(HOUR)
				fFILE=open(fName,"a")
				fFILE.write(str(line))
				fFILE.close()	
	
		except ValueError:
			pass



	os.remove(FILEBASE)
	b=time()
	c=b-a
	d=round(c,3)
	syslog.syslog("[p1_split] : done "+str(d)+" seconds.")
def isValidUrl(lang,artURL):
	BADLIST=wikicount.getBadList(lang)
	if artURL in BADLIST:
		return False
	else:
		return True
	

def p2_filter():
	syslog.syslog('[p2_filter] start')
	for lang in LANGLIST:
		a=time()
		VALIDS=0
		INVALIDS=0
		for FILENAME in glob.glob('/tmp/'+str(lang)+'_staging/q*'):
			DIRNAME='/tmp/'+str(lang)+'_staging/'
			if not os.path.exists(DIRNAME):
				print "creating new directory for "+str(lang)+"_staging"
				os.makedirs(DIRNAME)
			print FILENAME
			IFILE=open(FILENAME,"r")
	        	OUTFILENAME=string.replace(FILENAME,'staging','ondeck')
			IMGFILENAME=string.replace(FILENAME,'staging','image')
			CATFILENAME=string.replace(FILENAME,'staging','category')
			try:
				OFILE=open(OUTFILENAME,"w")
			except IOError:
				ODIR='/tmp/'+str(lang)+'_ondeck'
				if not os.path.exists(ODIR):
					print "create new for "+ODIR
					os.makedirs(ODIR)
				continue
				
			try:
				CATFILE=open(CATFILENAME,"a")
			except IOError:
				CATDIR='/tmp/'+str(lang)+'_category/'
				if not os.path.exists(CATDIR):
					print "create new dir for "+CATDIR
					os.makedirs(CATDIR)
				continue
			try:
				IMGFILE=open(IMGFILENAME,"a")
			except IOError:
				IMGDIR='/tmp/'+str(lang)+'_image'
				if not os.path.exists(IMGDIR):
					print "create new dir for "+IMGDIR
					os.makedirs(IMGDIR)
				continue
			for line in IFILE:
				record=line.strip().split()
				try:
					TITLE=record[1].decode('unicode_escape')
				except UnicodeDecodeError:
					continue
				if lang=='en':
					if isValidUrl(lang,record[1]):
						pass
					else:
						INVALIDS+=1
						continue
				TITLE=TITLE.strip('(){}')
				ptnTalk=re.search("Talk:",record[1])
				ptnUser=re.search("User:",record[1])
				ptnWiki=re.search("Wikipedia:",record[1])
				ptnSpecial=re.search("Special:",record[1])
				ptnUTalk=re.search("User_talk:",record[1])
				ptnTemplate=re.search("Template:",record[1])
				ptnWTalk=re.search("Wikpedia_talk:",record[1])
				ptnFile=re.search("Файл",record[1])
				ptnCategory=re.search("Категория",record[1])
				ptnCTalk=re.search("Category_talk:",record[1])
				ptnSTalk=re.search("talk:",record[1])
				ptnImage=re.search("Image:",record[1])
				ptnPhp=re.search(".php",record[1])
				if ptnCategory:
					CATFILE.write(line)
				if ptnImage or ptnFile:
					IMGFILE.write(line)
				if  not ptnImage and not ptnPhp and not ptnSpecial and not ptnTemplate and not ptnWTalk and not ptnFile and not ptnCategory and not ptnCTalk and not ptnSTalk:
					OFILE.write(line)
			OFILE.close()
			IMGFILE.close()
			CATFILE.close()
			os.remove(FILENAME)

		b=time()
		c=b-a
		d=round(c,3)
		syslog.syslog("p2_fi: Lang: "+str(lang)+" runtime: "+str(d)+" seconds. V:"+str(VALIDS)+" InV:"+str(INVALIDS))

def p2x_move_to_action():
	syslog.syslog("[p2x] - start")
	for lang in LANGLIST:
		FOLDER='/tmp/'+str(lang)+'_ondeck/q*'
		for FILENAME in glob.glob(FOLDER):
			IFILE=open(FILENAME,'r')
			OFILENAME=string.replace(FILENAME,'ondeck','action')
			OFILE=open(OFILENAME,'w')
			for line in IFILE: 
				OFILE.write(line)
			IFILE.close()
			OFILE.close() 
			os.remove(FILENAME)
	syslog.syslog("[p2x] - end")

def UpdateHits(FILEPROC,HOUR,DAY,MONTH,YEAR,LANG):
     HOURLYDB=str(LANG)+'_hitshourly'
     HOURDAYDB=str(LANG)+'_hitshourlydaily'
     HITSMAPDB=str(LANG)+'_hitsmap'
     HITSDAILY=str(LANG)+'_hitsdaily'
     connection=Connection()
     db=connection.wc
     RECORDS=0
     DAYKEY=str(YEAR)+"_"+str(MONTH)+"_"+str(DAY)
     for FILENAME in glob.glob(FILEPROC):
	print FILENAME	
	try:
		IFILE2=open(FILENAME,'r')
		for line in IFILE2:
			try:
		          line=line.strip().split()
		          HASH=hashlib.sha1(line[1]).hexdigest()
		          POSTFIND={'_id': HASH}
			  TITLESTRING=line[1].decode('utf-8')
	
				#hitshourly never deleted
			  db[HOURLYDB].update(POSTFIND,{"$inc":{HOUR:int(line[2])}},upsert=True)
				#hitshourlydaily wipes inverted hour every hour
			  db[HOURDAYDB].update(POSTFIND,{"$set":{HOUR:int(line[2])}},upsert=True)
			  db[HITSDAILY].update(POSTFIND,
					{"$inc":
						{DAYKEY: int(line[2])}
					,
					"$set":
						{'title':TITLESTRING}
					}	
					,upsert=True)

		          RECORDS+=1
			except UnicodeDecodeError: 
			  syslog.syslog("p3_add_to_db.py - UnicodeDecodeError")
			  pass
     		IFILE2.close()
		os.remove(FILENAME)
	except (NameError,IOError) as e:
		syslog.syslog("Error in "+str(FILENAME)+", P3_add_to_db.py stopping, "+str(e.strerror)+", errno "+str(e.errno) )
		pass
     FINAL="p3_add_to_db.py:total of %s records." % (str(RECORDS))
     syslog.syslog(FINAL)

def p3_add():
	conn=Connection()
	db=conn.wc
    	InvertHour=returnInvertedHour(HOUR)
	for lang in LANGLIST:
	    if WEEKDAY=='5':
	            STARTTIME=wikicount.fnStartTimer()
		    HOURDAYDB=str(lang)+'_hitshourlydaily'
		    db[HOURDAYDB].update({str(InvertHour):{'$exists':True}},{'$set':{str(InvertHour):0}},False,{'multi':True})
		    RUNTIME=wikicount.fnEndTimerCalcRuntime(STARTTIME)
		    syslog.syslog('p3_add: Old Hour '+str(HOUR)+' records reset to zero in '+str(RUNTIME)+' seconds. Now adding to db the current hour...')
	    ruFILE1="/tmp/"+str(lang)+"_action/q1_pagecounts.*"
	    ruFILE2="/tmp/"+str(lang)+"_action/q2_pagecounts.*"
	    ruFILE3="/tmp/"+str(lang)+"_action/q3_pagecounts.*"
	    ruFILE4="/tmp/"+str(lang)+"_action/q4_pagecounts.*"

	    STARTTIME=wikicount.fnStartTimer()
	    t = Process(target=UpdateHits, args=(ruFILE1,HOUR,DAY,MONTH,YEAR,lang))
	    u = Process(target=UpdateHits, args=(ruFILE2,HOUR,DAY,MONTH,YEAR,lang))
	    v = Process(target=UpdateHits, args=(ruFILE3,HOUR,DAY,MONTH,YEAR,lang))
	    w = Process(target=UpdateHits, args=(ruFILE4,HOUR,DAY,MONTH,YEAR,lang))
	
	    t.daemon=True
	    u.daemon=True
	    v.daemon=True
	    w.daemon=True

	    t.start()
	    u.start()
	    v.start()
	    w.start()

	    t.join()
	    u.join()
	    v.join()
	    w.join()
	    RUNTIME=wikicount.fnEndTimerCalcRuntime(STARTTIME)
    	    syslog.syslog('p3_add: Lang: '+str(lang)+' records added in '+str(RUNTIME)+' seconds.P3 Done now!')


def UpdateImages(FILEPROC,HOUR,DAY,MONTH,YEAR,LANG):
     IMAGECNAME=str(LANG)+"_image"
     IMAGECHNAME=str(LANG)+"_imagehourly"
     IMAGECDNAME=str(LANG)+"_imagedaily"
     connection=Connection()
     db=connection.wc
     RECORDS=0
     DAYKEY=str(YEAR)+"_"+str(MONTH)+"_"+str(DAY)
     for FILENAME in glob.glob(FILEPROC):	
	try:
		IFILE2=open(FILENAME,'r')
		for line in IFILE2:
			try:
		          line=line.strip().split()
		          HASH=hashlib.sha1(line[1]).hexdigest()
		          POSTFIND={'_id': HASH}
			  TITLESTRING=line[1].decode('utf-8')
		          db[IMAGECNAME].update(POSTFIND,{ "$inc" : { "Hits" : int(line[2]) },"$set":{'title':TITLESTRING} },upsert=True)
			  db[IMAGECHNAME].update(POSTFIND,{"$inc":{HOUR:int(line[2])}},upsert=True)
			  db[IMAGECDNAME].update(POSTFIND,{"$inc":{DAYKEY: int(line[2])}},upsert=True)
		          RECORDS+=1
			except UnicodeDecodeError: 
			  syslog.syslog("p3_add_to_db.py - UnicodeDecodeError")
			  pass
     		IFILE2.close()
		os.remove(FILENAME)
	except (NameError,IOError):
		syslog.syslog("Error encountered! P3_add_to_db.py stopping, NameError or IOError")
		pass
     FINAL=" processed a total of %s records." % (str(RECORDS))
     syslog.syslog(FINAL)

def p3_addImages():
    STARTTIME=wikicount.fnStartTimer()
    for lang in LANGUAGES:
	    FILEPROC2="/tmp/"+str(lang)+"_image/q2_pagecounts.*"
	    FILEPROC3="/tmp/"+str(lang)+"_image/q3_pagecounts.*"
	    FILEPROC4="/tmp/"+str(lang)+"_image/q4_pagecounts.*"
	    FILEPROC5="/tmp/"+str(lang)+"_image/q1_pagecounts.*"
	    p = Process(target=UpdateImages, args=(FILEPROC2,HOUR,DAY,MONTH,YEAR,str(lang)))
	    q = Process(target=UpdateImages, args=(FILEPROC3,HOUR,DAY,MONTH,YEAR,str(lang)))
	    r = Process(target=UpdateImages, args=(FILEPROC4,HOUR,DAY,MONTH,YEAR,str(lang)))
	    s = Process(target=UpdateImages, args=(FILEPROC5,HOUR,DAY,MONTH,YEAR,str(lang)))
	    p.daemon=True
	    q.daemon=True
	    r.daemon=True
	    s.daemon=True
	    p.start()
	    q.start()
	    r.start()
	    s.start()



	    p.join()
	    q.join()
	    r.join()
	    s.join()
	    RUNTIME=wikicount.fnEndTimerCalcRuntime(STARTTIME)
	    syslog.syslog("p3_image_add: runtime "+str(RUNTIME)+' seconds')
def p50_removeSpam():
	lang='en'
	LANGUAGES=wikicount.getLanguageList()
	conn=Connection()
	db=conn.wc
	COUNT=0
	for lang in LANGUAGES:
		SPAMLIST=wikicount.fnGetSpamList(lang)
		CNAMEHH=str(lang)+'_hitshourly'
		CNAMEHHD=str(lang)+'_hitshourlydaily'
		CNAMEHD=str(lang)+'_hitsdaily'
		for id in SPAMLIST:
			db[CNAMEHH].remove({'_id':str(id)})
			db[CNAMEHHD].remove({'_id':str(id)})
			db[CNAMEHD].remove({'_id':str(id)})
			COUNT+=1
	syslog.syslog("[p50] - "+str(COUNT)+" records removed.")
def p70export():
	STARTTIME=wikicount.fnStartTimer()
	syslog.syslog('p70_export.py: starting...')
	DAYKEY=str(YEAR)+"_"+str(MONTH)+"_"+str(DAY)
	OPTIONS=" -d wc -c hitsdaily -q '{\""+str(DAYKEY)+"\":{\"$exists\":true}}' --fields "+str(DAYKEY)+",\"_id\" --csv --out /root/mongo.csv"
	os.system("mongoexport "+str(OPTIONS))

	os.system("sed -i 1d /root/mongo.csv")
	for lang in LANGLIST:
		hdCOLL=str(lang)+"_hitsdaily"
		outfile="/root/"+str(lang)+"_mongo.csv"
		OPTIONS=" -d wc -c "+hdCOLL+" -q '{\""+str(DAYKEY)+"\":{\"$exists\":true}}' --fields "+str(DAYKEY)+",\"_id\" --csv --out "+str(outfile)
		os.system("mongoexport "+str(OPTIONS))
		os.system("sed -i 1d "+str(outfile))


	RUNTIME=wikicount.fnEndTimerCalcRuntime(STARTTIME)
	syslog.syslog('p70_export.py: runtime '+str(RUNTIME)+' seconds.')

def p80_sortMongoHD():
	STARTTIME=wikicount.fnStartTimer()
	syslog.syslog('p80_sortMongoHD: starting...')
	n=7  #number partitions to break into
	IFILE=open("/root/mongo.csv","r")
	SORTME=[]
	for lang in LANGLIST:
		IFILE=open("/root/"+str(lang)+"_mongo.csv","r")
		SORTME=[]
		for line in IFILE:
			line=line.strip('"').split(',')
			HASH=line[1].replace("\"","")
			rec=(line[0],HASH)
			SORTME.append(rec)
		IFILE.close()
		pconn,cconn=Pipe()
		lyst=[]
		p=Process(target=sorting.QuickSortMPListArray,args=(SORTME,cconn,n))
		p.start()
		lyst=pconn.recv()
		p.join()
		OFILE=open("/root/"+str(lang)+"_mongo.csv.sorted","w")
		for a in lyst:
			OFILE.write(str(a[0])+','+a[1])
		OFILE.close()

def p90_addTophits():
	STARTTIME=wikicount.fnStartTimer()
	syslog.syslog('tophits.py:  starting...')
	MONTHNAME=wikicount.fnGetMonthName()
	conn=Connection()
	db=conn.wc
	RECCOUNT=1
	DAYKEY=str(YEAR)+"_"+str(MONTH)+"_"+str(DAY)


	LANGLIST=wikicount.getLanguageList()
	for lang in LANGLIST:
		PLACEMAP=str(lang)+"_mapPlace"
		HITSMAP=str(lang)+"_mapHits"
		try:
			IFILE=open("/root/"+str(lang)+"_mongo.csv.sorted","r")
		except IOError:
			syslog.syslog("Error opening file for "+str(lang))
			continue
		RESULT=[]
		RECCOUNT=0
		for line in IFILE:
	    		if RECCOUNT < 1000:
        			line=line.strip().split(",")
	        		RESULT.append((line[0],line[1]))
        			RECCOUNT+=1
	        		try:
					db[PLACEMAP].update({'_id':str(line[1])},{"$set":{DAYKEY:RECCOUNT}},upsert=True)
					db[HITSMAP].update({'_id':str(line[1])},{"$set":{DAYKEY:int(line[0])}},upsert=True)
				except TypeError:
	    				pass
	   	 	else:
	        		break
	IFILE.close()

	RUNTIME=wikicount.fnEndTimerCalcRuntime(STARTTIME)
	syslog.syslog('tophits.py: runtime is '+str(RUNTIME)+' seconds.')

def p99_threehrrolling():
	syslog.syslog("Entering p99_threehrrolling")
	DAY,MONTH,YEAR,HOUR,expiretime=wikicount.fnReturnTimes()
	STARTTIME=wikicount.fnStartTimer()
	conn=Connection()
	db=conn.wc
	HOUR,HOUR2,HOUR3=wikicount.fnReturnLastThreeHours(int(HOUR))
	HOUR=wikicount.fnStrFmtDate(HOUR)
	HOUR2=wikicount.fnStrFmtDate(HOUR2)
	HOUR3=wikicount.fnStrFmtDate(HOUR3)
	hourlies=[]
	TypeErrors=0
	KeyErrors=0
	z=1
	LANGUAGES=wikicount.getLanguageList()
	for lang in LANGUAGES:
		hourlies=[]
		KeyErrors=0
		TypeErrors=0
		hhdTABLE=str(lang)+"_hitshourlydaily"
		hdTABLE=str(lang)+"_hitsdaily"
		outTABLE=str(lang)+"_threehour"
		RESULTS=db[hhdTABLE].find({str(HOUR):{'$exists':True}}).sort(str(HOUR),-1).limit(200)
		syslog.syslog(str(lang)+" : "+str(RESULTS.count()))
		for item in RESULTS:
			try:
				vB1=False
				vB2=False
				vB3=False
		                QUERYtitle=db[hdTABLE].find_one({'_id':item['_id']})
		                atitle=QUERYtitle['title']
		                title,utitle=wikicount.FormatName(atitle)
		                try:
		                        b1=item[HOUR]
					vB1=True
		                except KeyError:
		                        b1=0
		                try:
		                        b2=item[HOUR2]
					vB2=True
		                except KeyError:
		                        b2=0
		                try:
		                        b3=item[HOUR3]
					vB3=True
		                except KeyError:
		                        b3=0
				if vB1 and vB2 and vB3:
		        	        rollingavg=mean(array([b1,b2,b3]))
				elif vB1 and vB2:
					rollingavg=mean(array([b1,b2]))
				elif vB1 and vB3:
					rollingavg=mean(array([b1,b3]))
				elif vB1:
					rollingavg=b1
				elif vB2 and vB3:
					rollingavg=mean(array([b3,b2]))
#				elif vB2:
#					rollingavg=b2
				else:
					rollingavg=0
#					rollingavg=b3
		
		                rec={'title':atitle,'rollavg':int(rollingavg),'id':item['_id']}
		                hourlies.append(rec)
		        except TypeError:
		                TypeErrors+=1
		        except KeyError:
		                KeyErrors+=1
		z=1
		db[outTABLE].remove()
		for w in sorted(hourlies,key=itemgetter('rollavg'),reverse=True):
		        if z < 101:
		                rec={'place':z,'title':w['title'],'rollavg':w['rollavg'],'id':w['id']}
		                db[outTABLE].insert(rec)
		                z+=1
	
		syslog.syslog("[p99_end_3hrrollavg] - Lang: "+str(lang)+" TypeErrors: "+str(TypeErrors)+" KeyErrors: "+str(KeyErrors))

p0_dl()
p1_split()
p2_filter()
p2x_move_to_action()
p3_add()
p3_addImages()
p50_removeSpam()


p99_threehrrolling()
syslog.syslog("Master.py - All Functions complete!")
