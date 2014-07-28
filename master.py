#!/usr/bin/python
#coding: utf-8
"""The master.py file downloads the hourly log file from wikipedia,
    parses it, and adds the hourly hit data to a MongoDB server instance.

    """
import urllib2
import os
import gzip
import syslog
import glob
import re
import string
from multiprocessing import Process
import hashlib
from datetime import date
import time
from pymongo import Connection
from lib import sorting, wikicount


FILEBASE = "/tmp/staging/pagecounts.tmp"
DAY, MONTH, YEAR, HOUR, expiretime = wikicount.fnReturnTimes()
HOUR = wikicount.minusHour(int(HOUR))
HOUR = wikicount.minusHour(int(HOUR))
DAY, MONTH, HOUR = wikicount.fnFormatTimes(DAY, MONTH, HOUR)
LANGLIST = wikicount.getLanguageList()
LANGUAGES = wikicount.getLanguageList()
vTODAY = date.today()
vDOW = vTODAY.weekday()

TOTAL_RECORDS_UPDATED = 0

WEEKDAY = time.strftime("%w")
from time import time


def returnInvertedHour(HOUR):
    """ This returns the hour farthest away, for clearing the hitshourly table.
    """
    if int(HOUR) < 12:
        return str(int(HOUR)+12)
    elif int(HOUR) > 11:
        return str(int(HOUR)-12)


def p0_dl():
    """
    Does the dl from dumps.wikimedia.org.  Will search minute by minute, starting at zero, to 60
    looking the correct url for the current hour.
    """
    URL = "http://dumps.wikimedia.org/other/pagecounts-raw/"+str(YEAR)+"/"+str(YEAR)+"-"+str(MONTH)+"/pagecounts-"
    URLDATE = str(YEAR)+str(MONTH)+str(DAY)
    URLSUFFIX = "-"+str(HOUR)+"*.gz"
    URL += URLDATE
    URL += URLSUFFIX

    #Now with URL, download file
    a =time()
    missing = True
    MINUTESEARCH=0
    while missing and MINUTESEARCH<60:
        MINUTE = wikicount.fnFormatHour(MINUTESEARCH)
        URL = "http://dumps.wikimedia.org/other/pagecounts-raw/"+str(YEAR)+"/"+str(YEAR)+"-"+str(MONTH)+"/pagecounts-"
        #print HOUR
        URLDATE = str(YEAR)+str(MONTH)+str(DAY)
        URLSUFFIX = "-"+str(HOUR)+"00"+str(MINUTE)+".gz"
        URL += URLDATE
        URL += URLSUFFIX
        MINUTESEARCH += 1
        try:
            print URL
            COUNTFILE=urllib2.urlopen(URL)
            if COUNTFILE.code==200:
                fetchURL=URL
                print "fetchURL: "+str(fetchURL)
                missing=False
        except urllib2.HTTPError, e:
            print e.fp.read()

    if not missing:
        COUNTFILE=urllib2.urlopen(fetchURL)
        OFILE=open(FILEBASE,"w")
        OFILE.write(COUNTFILE.read())
        OFILE.close()
        COUNTFILE=urllib2.urlopen(fetchURL)
        CFILE=open("pagecounts.tmp.gz","w")

        CFILE.write(COUNTFILE.read())
        CFILE.close()
        b=time()
        c=b-a
        d=round(c,3)
        syslog.syslog("[master.py][p0_dl] Successful download of "+str(URL)+" in "+str(d)+" seconds.")
    else:
        syslog.syslog("[master.py][p0_dl] 404 not found error")



def p1_split():
    a=time()
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
    syslog.syslog("[master.py][p1_split] done in "+str(d)+" seconds. Successes: "+str(SUCCESS)+" Exceptions: "+str(EXCEPTS))
def isValidUrl(lang,artURL):
    BADLIST= wikicount.getBadList(lang)
    if artURL in BADLIST:
        return False
    else:
        return True


def p2_filter():
    for lang in LANGLIST:
        a=time()
        RECS=0
        RECERRS=0
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
                    RECERRS+=1
                    continue
                if lang=='en':
                    if isValidUrl(lang,record[1]):
                        VALIDS+=1
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
                    RECS+=1
                if ptnImage or ptnFile:
                    IMGFILE.write(line)
                    RECS+=1
                if  not ptnImage and not ptnPhp and not ptnSpecial and not ptnTemplate and not ptnWTalk and not ptnFile and not ptnCategory and not ptnCTalk and not ptnSTalk:
                    OFILE.write(line)
                    RECS+=1
            OFILE.close()
            IMGFILE.close()
            CATFILE.close()
            os.remove(FILENAME)

        b=time()
        c=b-a
        d=round(c,3)
        syslog.syslog("[master.py][p2_filter] Lang: "+str(lang)+" runtime: "+str(d)+" seconds. Recs written: "+str(RECS)+" Record errors: "+str(RECERRS))

def p2x_move_to_action():
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

def UpdateHits(FILEPROC,HOUR,DAY,MONTH,YEAR,LANG):
    UPDATED=0
    EXCEPTS=0
    HOURLYDB=str(LANG)+'_hitshourly'
    HOURDAYDB=str(LANG)+'_hitshourlydaily'
    HITSMAPDB=str(LANG)+'_hitsmap'
    HITSDAILY=str(LANG)+'_hitsdaily'
    connection=Connection()
    db=connection.wc
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

              UPDATED+=1
            except UnicodeDecodeError:
              EXCEPTS+=1
              continue
            IFILE2.close()
        os.remove(FILENAME)
    except (NameError,IOError) as e:
     	EXCEPTS+=1
     	syslog.syslog("[master.py][UpdateHits] (thread) complete. Lang: "+str(LANG)+" Updated: "+str(UPDATED)+" Exceptions: "+str(EXCEPTS))

def p3_add():
    conn=Connection()
    db=conn.wc
    InvertHour=returnInvertedHour(HOUR)
    for lang in LANGLIST:
        if WEEKDAY=='5':
            STARTTIME= wikicount.fnStartTimer()
            HOURDAYDB=str(lang)+'_hitshourlydaily'
            db[HOURDAYDB].update({str(InvertHour):{'$exists':True}},{'$set':{str(InvertHour):0}},False,{'multi':True})
            RUNTIME= wikicount.fnEndTimerCalcRuntime(STARTTIME)
            syslog.syslog('[master.py][p3_add] Old Hour '+str(InvertHour)+' records reset to zero in '+str(RUNTIME)+' seconds. ')
        ruFILE1="/tmp/"+str(lang)+"_action/q1_pagecounts.*"
        ruFILE2="/tmp/"+str(lang)+"_action/q2_pagecounts.*"
        ruFILE3="/tmp/"+str(lang)+"_action/q3_pagecounts.*"
        ruFILE4="/tmp/"+str(lang)+"_action/q4_pagecounts.*"

        STARTTIME= wikicount.fnStartTimer()
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
        RUNTIME= wikicount.fnEndTimerCalcRuntime(STARTTIME)


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
			  pass
     		IFILE2.close()
		os.remove(FILENAME)
	except (NameError,IOError):
		pass
     #syslog.syslog("[master.py][UpdateImages] Language: "+str(LANG)+" Records: "+str(RECORDS))

def p3_addImages():
    STARTTIME= wikicount.fnStartTimer()
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
	    RUNTIME= wikicount.fnEndTimerCalcRuntime(STARTTIME)
def p50_removeSpam():
	lang='en'
	LANGUAGES= wikicount.getLanguageList()
	conn=Connection()
	db=conn.wc
	COUNT=0
	for lang in LANGUAGES:
		SPAMLIST= wikicount.fnGetSpamList(lang)
		CNAMEHH=str(lang)+'_hitshourly'
		CNAMEHHD=str(lang)+'_hitshourlydaily'
		CNAMEHD=str(lang)+'_hitsdaily'
        for id in SPAMLIST:
            db[CNAMEHH].remove({'_id':str(id)})
            db[CNAMEHHD].remove({'_id':str(id)})
            db[CNAMEHD].remove({'_id':str(id)})
            COUNT+=1
    	syslog.syslog("[master.py][p50] "+str(COUNT)+" records removed.")
def p70export():
    STARTTIME= wikicount.fnStartTimer()
    syslog.syslog('p70_export.py: starting...')
    DAYKEY=str(YEAR)+"_"+str(MONTH)+"_"+str(DAY)
    OPTIONS=" -d wc -c hitsdaily -q '{\""+str(DAYKEY)+"\":{\"$exists\":true}}' --fields "+str(DAYKEY)+",\"_id\" --csv --out mongo.csv"
    os.system("mongoexport "+str(OPTIONS))

    os.system("sed -i 1d mongo.csv")
    for lang in LANGLIST:
        hdCOLL=str(lang)+"_hitsdaily"
        outfile=""+str(lang)+"_mongo.csv"
        OPTIONS=" -d wc -c "+hdCOLL+" -q '{\""+str(DAYKEY)+"\":{\"$exists\":true}}' --fields "+str(DAYKEY)+",\"_id\" --csv --out "+str(outfile)
        os.system("mongoexport "+str(OPTIONS))
        os.system("sed -i 1d "+str(outfile))


    RUNTIME= wikicount.fnEndTimerCalcRuntime(STARTTIME)
    syslog.syslog('p70_export.py: runtime '+str(RUNTIME)+' seconds.')

def p80_sortMongoHD():
    syslog.syslog('p80_sortMongoHD: starting...')

    for lang in LANGLIST:
        IFILE=open(""+str(lang)+"_mongo.csv","r")
        SORTME=[]
        for line in IFILE:
            line=line.strip('"').split(',')
            HASH=line[1].replace("\"","")
            rec=(line[0],HASH)
            SORTME.append(rec)
        IFILE.close()
        lyst=[]
        lyst=sorting.QuickSortListArray(SORTME)
        OFILE=open(""+str(lang)+"_mongo.csv.sorted","w")
        for a in lyst:
            OFILE.write(str(a[0])+','+a[1])
        OFILE.close()

def p90_addTophits():
    syslog.syslog('tophits.py:  starting...')
    conn=Connection()
    db=conn.wc
    DAYKEY=str(YEAR)+"_"+str(MONTH)+"_"+str(DAY)


    LANGLIST= wikicount.getLanguageList()
    for lang in LANGLIST:
        PLACEMAP=str(lang)+"_mapPlace"
        HITSMAP=str(lang)+"_mapHits"
        try:
            IFILE=open(""+str(lang)+"_mongo.csv.sorted","r")
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

    RUNTIME= wikicount.fnEndTimerCalcRuntime(STARTTIME)
    syslog.syslog('tophits.py: runtime is '+str(RUNTIME)+' seconds.')

syslog.syslog("[master.py][main] Started")			
p0_dl()
p1_split()
p2_filter()
p2x_move_to_action()
p3_add()
p3_addImages()
p50_removeSpam()


syslog.syslog("[master.py][main] Master.py - All Functions complete!")
