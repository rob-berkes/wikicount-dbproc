#!/usr/bin/python
#coding: utf-8
#from pycassa.pool import ConnectionPool
#from pycassa.columnfamily import ColumnFamily
import cql
import urllib2
import os
import gzip 
from functions import wikicount
import syslog
import glob
import string
from multiprocessing import Process
import hashlib
from time import time



#colfam_hourly=ColumnFamily(connection,'hourly')
#colfam_daily=ColumnFamily(connection,'daily')
#colfam_hd=ColumnFamily(connection,'hourlydaily')
FILEBASE="/tmp/staging/pagecounts.tmp"
DAY,MONTH,YEAR,HOUR,expiretime=wikicount.fnReturnTimes()
HOUR=wikicount.minusHour(int(HOUR))
HOUR=wikicount.minusHour(int(HOUR))
DAY,MONTH,HOUR=wikicount.fnFormatTimes(DAY,MONTH,HOUR)
LANGLIST=wikicount.getLanguageList()
vUPDATED=0


def p0_dl():
    print 'p0 starting!'
    URL="http://dumps.wikimedia.org/other/pagecounts-raw/"+str(YEAR)+"/"+str(YEAR)+"-"+str(MONTH)+"/pagecounts-"
    URLDATE=str(YEAR)+str(MONTH)+str(DAY)
    URLSUFFIX="-"+str(HOUR)+"*.gz"
    URL+=URLDATE
    URL+=URLSUFFIX
    a=time()
    missing=True
    MINUTESEARCH=0
    while missing and MINUTESEARCH<60:
        MINUTE=wikicount.fnFormatHour(MINUTESEARCH)
        URL="http://dumps.wikimedia.org/other/pagecounts-raw/"+str(YEAR)+"/"+str(YEAR)+"-"+str(MONTH)+"/pagecounts-"
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
        except urllib2.HTTPError, e:
            print e.fp.read()
    if not missing:
        COUNTFILE=urllib2.urlopen(fetchURL)
        OFILE=open(FILEBASE,"w")
        OFILE.write(COUNTFILE.read())
        OFILE.close()
        COUNTFILE=urllib2.urlopen(fetchURL)
        CFILE=open("/tmp/pagecounts.tmp.gz","w")
        CFILE.write(COUNTFILE.read())
        CFILE.close()
        b=time()
        c=b-a
        d=round(c,3)
        print "successful download! now on to p1"
        syslog.syslog("[master.py][p0_dl] Successful download of "+str(URL)+" in "+str(d)+" seconds.")
    else:
        syslog.syslog("[master.py][p0_dl] 404 not found error")



def p1_split():
    IFILE=gzip.open(FILEBASE,"r")
    NUMBERLOGFILES=6
    COUNTTHRESHOLD=2
    RECCOUNT=0
    for line in IFILE:
        record=line.strip().split()
        record[0]=record[0].strip('.')
        RECCOUNT+=1
        try:
            if record[0] in LANGLIST and int(record[2]) > COUNTTHRESHOLD and RECCOUNT % NUMBERLOGFILES==0:
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
            elif record[0] in LANGLIST and int(record[2])>COUNTTHRESHOLD and RECCOUNT % NUMBERLOGFILES == 4:
                fName='/tmp/'+str(record[0])+'_staging/q5_pagecounts.'+str(HOUR)
                fFILE=open(fName,"a")
                fFILE.write(str(line))
                fFILE.close()
            elif record[0] in LANGLIST and int(record[2])>COUNTTHRESHOLD and RECCOUNT % NUMBERLOGFILES == 5:
                fName='/tmp/'+str(record[0])+'_staging/q6_pagecounts.'+str(HOUR)
                fFILE=open(fName,"a")
                fFILE.write(str(line))
                fFILE.close()
        except ValueError:
            pass
    os.remove(FILEBASE)
    print "p1 successful, now on to p2!"

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
            try:
                OFILE=open(OUTFILENAME,"w")
            except IOError:
                ODIR='/tmp/'+str(lang)+'_ondeck'
                if not os.path.exists(ODIR):
                    print "create new for "+ODIR
                    os.makedirs(ODIR)
                continue

            for line in IFILE:
                record=line.strip().split()
                try:
                    TITLE=record[1].decode('unicode_escape')
                except UnicodeDecodeError:
                    RECERRS+=1
                    continue
                OFILE.write(line)
                RECS+=1
            OFILE.close()
            os.remove(FILENAME)

        b=time()
        c=b-a
        d=round(c,3)
        print 'p2 successful, now moving!'
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
    print 'p2x successful now on to p3'
def UpdateHits(FILEPROC,HOUR,LANG,SERVERADDY):
     connection=cql.connect(SERVERADDY,9160,'wikitrends',cql_version='3.0.0')
     cursor=connection.cursor()
     UPDATED=0
     EXCEPTS=0
     for FILENAME in glob.glob(FILEPROC):
        print FILENAME
        try:
            IFILE2=open(FILENAME,'r')
            for line in IFILE2:
                try:
                    line=line.strip().split()
                    HASH=hashlib.sha1(line[1]).hexdigest()
                    irec={HOUR:str(line[2])}
                    I1="""
                        INSERT INTO hourly (id,hour,hits) VALUES ('HASH',HOUR,HITS)
                        ;"""
                    I1=I1.replace('HASH',str(HASH))
                    I1=I1.replace('HOUR',str(HOUR))
                    I1=I1.replace('HITS',str(line[2]))
                    r=cursor.execute(I1)
 #                   colfam_daily.insert(HASH,irec)
 #                   colfam_hd.insert(HASH,irec)
                    UPDATED+=1
                except UnicodeDecodeError:
                    EXCEPTS+=1
                    continue
            IFILE2.close()
            os.remove(FILENAME)
        except (NameError,IOError) as e:
            EXCEPTS+=1
            break
            #continue
     print str(UPDATED)+" updates done!"
     syslog.syslog("[master.py][UpdateHits] (thread) complete. Lang: "+str(LANG)+" Updated: "+str(UPDATED)+" Exceptions: "+str(EXCEPTS))

def p3_add():
    for lang in LANGLIST:

        ruFILE1="/tmp/"+str(lang)+"_action/q1_pagecounts.*"
        ruFILE2="/tmp/"+str(lang)+"_action/q2_pagecounts.*"
        ruFILE3="/tmp/"+str(lang)+"_action/q3_pagecounts.*"
        ruFILE4="/tmp/"+str(lang)+"_action/q4_pagecounts.*"
        ruFILE5="/tmp/"+str(lang)+"_action/q5_pagecounts.*"
        ruFILE6="/tmp/"+str(lang)+"_action/q6_pagecounts.*"

        STARTTIME=wikicount.fnStartTimer()
        t = Process(target=UpdateHits, args=(ruFILE1,HOUR,lang,'127.0.0.1'))
        u = Process(target=UpdateHits, args=(ruFILE2,HOUR,lang,'10.10.0.21'))
        v = Process(target=UpdateHits, args=(ruFILE3,HOUR,lang,'10.10.0.102'))
        w = Process(target=UpdateHits, args=(ruFILE4,HOUR,lang,'10.10.0.254'))
        x = Process(target=UpdateHits, args=(ruFILE5,HOUR,lang,'127.0.0.1'))
        y = Process(target=UpdateHits, args=(ruFILE6,HOUR,lang,'10.10.0.102'))

        t.daemon=True
        u.daemon=True
        v.daemon=True
        w.daemon=True
        x.daemon=True
        y.daemon=True

        t.start()
        u.start()
        v.start()
        w.start()
        x.start()
        y.start()

        t.join()
        u.join()
        v.join()
        w.join()
        x.join()
        y.join()
        RUNTIME=wikicount.fnEndTimerCalcRuntime(STARTTIME)
    print "p3 done!"
syslog.syslog("[master.py][main] Started")			
p0_dl()
p1_split()
p2_filter()
p2x_move_to_action()
syslog.syslog("p3_add] p3_add starting now")
p3_add()

syslog.syslog("[master.py][main] Master.py - All Functions complete!")
