#!/usr/bin/python
# coding: utf-8
"""The master.py file downloads the hourly log file from wikipedia,
    parses it, and adds the hourly hit data to a MongoDB server instance.
    """
import urllib2
import os
import gzip
import glob
import re
import string
from multiprocessing import Process
import hashlib
from datetime import date
from pymongo import Connection
from lib import wikicount
import cProfile
import pstats
import smtplib
from email.mime.text import MIMEText
import time

SYSOUT = "/tmp/zSYSOUT"
EMAILOUT = "/tmp/zEMAIL"
EMWFP = open(EMAILOUT,'w')
FILEBASE = "/tmp/staging/pagecounts.tmp"
DAY, MONTH, YEAR, HOUR, expiretime = wikicount.fnReturnTimes()
HOUR = wikicount.minusHour(int(HOUR))
#HOUR = wikicount.minusHour(int(HOUR))
DAY, MONTH, HOUR = wikicount.fnFormatTimes(DAY, MONTH, HOUR)
LANGLIST = wikicount.LList
vTODAY = date.today()
vDOW = vTODAY.weekday()
SYSOUT = '/tmp/sysout'
TOTAL_RECORDS_UPDATED = 0
WEEKDAY = time.strftime("%w")
from time import time


def returnInvertedHour(HOUR):
    """ This returns the hour farthest away, for clearing the hitshourly table.
    """
    if int(HOUR) < 12:
        return str(int(HOUR) + 12)
    elif int(HOUR) > 11:
        return str(int(HOUR) - 12)


def p0_dl():
    """
    Does the dl from dumps.wikimedia.org.  Will search minute by minute, starting at zero, to 60
    looking the correct url for the current hour.
    """
    URL = "http://dumps.wikimedia.org/other/pagecounts-raw/" + str(YEAR) + "/" + str(YEAR) + "-" + str(
        MONTH) + "/pagecounts-"
    URLDATE = str(YEAR) + str(MONTH) + str(DAY)
    URLSUFFIX = "-" + str(HOUR) + "*.gz"
    URL += URLDATE
    URL += URLSUFFIX
    SYSFILE = open(SYSOUT, 'w')
    #Now with URL, download file
    a = time()
    missing = True
    MINUTESEARCH = 0
    while missing and MINUTESEARCH < 60:
        MINUTE = wikicount.fnFormatHour(MINUTESEARCH)
        URL = "http://dumps.wikimedia.org/other/pagecounts-raw/" + str(YEAR) + "/" + str(YEAR) + "-" + str(
            MONTH) + "/pagecounts-"
        #print HOUR
        URLDATE = str(YEAR) + str(MONTH) + str(DAY)
        URLSUFFIX = "-" + str(HOUR) + "00" + str(MINUTE) + ".gz"
        URL += URLDATE
        URL += URLSUFFIX
        MINUTESEARCH += 1
        try:
            #print URL
            COUNTFILE = urllib2.urlopen(URL)
            if COUNTFILE.code == 200:
                fetchURL = URL
                print "fetchURL: " + str(fetchURL)
                missing = False
        except urllib2.HTTPError, e:
            print e.fp.read()

    if not missing:
        COUNTFILE = urllib2.urlopen(fetchURL)
        OFILE = open(FILEBASE, "w")
        OFILE.write(COUNTFILE.read())
        OFILE.close()
        COUNTFILE = urllib2.urlopen(fetchURL)
        CFILE = open("pagecounts.tmp.gz", "w")
        CFILE.write(COUNTFILE.read())
        CFILE.close()
        b = time()
        c = b - a
        d = round(c, 3)
        SYSFILE.write("[master.py][p0_dl] Successful download of " + str(URL) + " in " + str(d) + " seconds.")
    else:
        SYSFILE.write("[master.py][p0_dl] 404 not found error")
    SYSFILE.close()


def p1_split():
    a = time()
    IFILE = gzip.open(FILEBASE, "r")
    SYSFILE = open(SYSOUT, 'a')

    NUMBERLOGFILES = 4
    RECCOUNT = 0
    SUCCESS = 0
    EXCEPTS = 0
    for line in IFILE:
        record = line.strip().split()
        LANGUAGEUSED = record[0].strip('.').lower()
        RECCOUNT += 1
        RECMOD = RECCOUNT % NUMBERLOGFILES
        RECMODFSTR = str(RECMOD+1)
	ffile = 'q'+RECMODFSTR+'_pagecounts.'+str(HOUR)
        fdir = '/tmp/' + str(LANGUAGEUSED) + '_staging/'
        fName = fdir+ffile
        try:
         fFILE = open(fName, "a")
	except IOError:
	 try:
	  fFILE = open(fName, "w")
	 except IOError:
	   # Some come thru invalid, reject
	   continue
	  
        fFILE.write(str(line))
        fFILE.close()


    try:
        os.remove(FILEBASE)
    except OSError:
        pass

    b = time()
    c = b - a
    d = round(c, 3)
    SYSFILE.write(
        "[master.py][p1_split] done in " + str(d) + " seconds. Successes: " + str(SUCCESS) + " Exceptions: " + str(
            EXCEPTS))
    SYSFILE.close()



def p2_filter():
    for lang in LANGLIST:
        a = time()
        RECS = 0
        RECERRS = 0
        for FILENAME in glob.glob('/tmp/' + str(lang) + '_staging/q*'):
            DIRNAME = '/tmp/' + str(lang) + '_staging/'
            if not os.path.exists(DIRNAME):
                print "creating new directory for " + str(lang) + "_staging"
                os.makedirs(DIRNAME)
            print FILENAME
            IFILE = open(FILENAME, "r")
            OUTFILENAME = string.replace(FILENAME, 'staging', 'ondeck')
            IMGFILENAME = string.replace(FILENAME, 'staging', 'image')
            CATFILENAME = string.replace(FILENAME, 'staging', 'category')
            try:
                OFILE = open(OUTFILENAME, "w")
            except IOError:
                ODIR = '/tmp/' + str(lang) + '_ondeck'
                if not os.path.exists(ODIR):
                    print "create new for " + ODIR
                    os.makedirs(ODIR)
                continue

            try:
                CATFILE = open(CATFILENAME, "a")
            except IOError:
                CATDIR = '/tmp/' + str(lang) + '_category/'
                if not os.path.exists(CATDIR):
                    print "create new dir for " + CATDIR
                    os.makedirs(CATDIR)
                continue
            try:
                IMGFILE = open(IMGFILENAME, "a")
            except IOError:
                IMGDIR = '/tmp/' + str(lang) + '_image'
                if not os.path.exists(IMGDIR):
                    print "create new dir for " + IMGDIR
                    os.makedirs(IMGDIR)
                continue
            for line in IFILE:
                record = line.strip().split()
                ptnSpecial = re.search("Special:", record[1])
                ptnTemplate = re.search("Template:", record[1])
                ptnWTalk = re.search("Wikpedia_talk:", record[1])
                ptnFile = re.search("Файл", record[1])
                ptnCategory = re.search("Категория", record[1])
                ptnCTalk = re.search("Category_talk:", record[1])
                ptnSTalk = re.search("talk:", record[1])
                ptnImage = re.search("Image:", record[1])
                ptnPhp = re.search(".php", record[1])
                if ptnCategory:
                    CATFILE.write(line)
                    RECS += 1
                if ptnImage or ptnFile:
                    IMGFILE.write(line)
                    RECS += 1
                if not ptnImage and not ptnPhp and not ptnSpecial \
                        and not ptnTemplate \
                        and not ptnWTalk and not ptnFile and not ptnCategory \
                        and not ptnCTalk and not ptnSTalk:
                    OFILE.write(line)
                    RECS += 1
            OFILE.close()
            IMGFILE.close()
            CATFILE.close()
            os.remove(FILENAME)

        b = time()
        c = b - a
        d = round(c, 3)
        SYSFILE = open(SYSOUT, 'a')
        SYSFILE.write(
            "[master.py][p2_filter] Lang: " + str(lang) + " runtime: " + str(d) + " seconds. Recs written: " + str(
                RECS) + " Record errors: " + str(RECERRS))
        SYSFILE.close()


def p2x_move_to_action():
    for lang in LANGLIST:
        FOLDER = '/tmp/' + str(lang) + '_ondeck/q*'
        for FILENAME in glob.glob(FOLDER):
            IFILE = open(FILENAME, 'r')
            OFILENAME = string.replace(FILENAME, 'ondeck', 'action')
            OFILE = open(OFILENAME, 'w')
            for line in IFILE:
                OFILE.write(line)
            IFILE.close()
            OFILE.close()
            os.remove(FILENAME)


def UpdateHits(FILEPROC, HOUR, DAY, MONTH, YEAR, LANG):
    SYSFILE = open(SYSOUT, 'a')
    UPDATED = 0
    EXCEPTS = 0
    HOURLYDB = str(LANG) + '_hitshourly'
    HOURDAYDB = str(LANG) + '_hitshourlydaily'
    HITSDAILY = str(LANG) + '_hitsdaily'
    connection = Connection()
    db = connection.wc
    DAYKEY = str(YEAR) + "_" + str(MONTH) + "_" + str(DAY)
    for FILENAME in glob.glob(FILEPROC):
        print FILENAME
    try:
        IFILE2 = open(FILENAME, 'r')
        for line in IFILE2:
            try:
                line = line.strip().split()
                HASH = hashlib.sha1(line[1]).hexdigest()
                POSTFIND = {'_id': HASH}
                TITLESTRING = line[1].decode('utf-8')
                #hitshourly never deleted
                db[HOURLYDB].update(POSTFIND, {"$inc": {HOUR: int(line[2])}}, upsert=True)
                #hitshourlydaily wipes inverted hour every hour
                db[HOURDAYDB].update(POSTFIND, {"$set": {HOUR: int(line[2])}}, upsert=True)
                db[HITSDAILY].update(POSTFIND,
                                     {"$inc":
                                          {DAYKEY: int(line[2])}
                                         ,
                                      "$set":
                                          {'title': TITLESTRING}
                                     }
                                     , upsert=True)

                UPDATED += 1
            except UnicodeDecodeError:
                EXCEPTS += 1
                continue
        IFILE2.close()
        os.remove(FILENAME)
    except (NameError, IOError) as e:
        EXCEPTS += 1
        SYSFILE.write("[master.py][UpdateHits] (thread) complete. Lang: " + str(LANG) + " Updated: " + str(
            UPDATED) + " Exceptions: " + str(EXCEPTS))
        SYSFILE.close()


def p3_add():
    conn = Connection()
    db = conn.wc
    InvertHour = returnInvertedHour(HOUR)
    for lang in LANGLIST:
        if WEEKDAY == '5':
            HOURDAYDB = str(lang) + '_hitshourlydaily'
            db[HOURDAYDB].update({str(InvertHour): {'$exists': True}}, {'$set': {str(InvertHour): 0}}, False,
                                 {'multi': True})
        ruFILE1 = "/tmp/" + str(lang) + "_action/q1_pagecounts.*"
        ruFILE2 = "/tmp/" + str(lang) + "_action/q2_pagecounts.*"
        ruFILE3 = "/tmp/" + str(lang) + "_action/q3_pagecounts.*"
        ruFILE4 = "/tmp/" + str(lang) + "_action/q4_pagecounts.*"

        t = Process(target=UpdateHits, args=(ruFILE1, HOUR, DAY, MONTH, YEAR, lang))
        u = Process(target=UpdateHits, args=(ruFILE2, HOUR, DAY, MONTH, YEAR, lang))
        v = Process(target=UpdateHits, args=(ruFILE3, HOUR, DAY, MONTH, YEAR, lang))
        w = Process(target=UpdateHits, args=(ruFILE4, HOUR, DAY, MONTH, YEAR, lang))

        t.daemon = True
        u.daemon = True
        v.daemon = True
        w.daemon = True

        t.start()
        u.start()
        v.start()
        w.start()

        t.join()
        u.join()
        v.join()
        w.join()


p0_dl()
cProfile.run('p1_split()',SYSOUT)
cProfile.run('p2_filter()',SYSOUT)
cProfile.run('p2x_move_to_action()',SYSOUT)
cProfile.run('p3_add()',SYSOUT)

p = pstats.Stats(SYSOUT,stream=EMWFP)
p.sort_stats('time').print_stats(20)
p.sort_stats('name').print_stats()
EMWFP.close()

EMFP = open(EMAILOUT,'rb')
msg = MIMEText(EMFP.read())
EMFP.close()

msg['Subject'] = 'Wikitrends.info: Hourly Mongodb download and parsing report'
msg['From'] = 'masterpy@wikitrends.info'
msg['To'] = 'rob.berkes@gmail.com'

s = smtplib.SMTP('localhost')
s.sendmail('masterpy@wikitrends.info','rob.berkes@gmail.com',msg.as_string())
s.quit()
