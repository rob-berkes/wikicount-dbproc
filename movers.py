#!/usr/bin/python
#coding: utf-8
import syslog

from pymongo import Connection
from lib import sorting, wikicount


def p98_todaysmovers():
	DAY,MONTH,YEAR,HOUR,expiretime= wikicount.fnReturnTimes()
	YDAY=DAY
	YMONTH=MONTH
	if DAY==1:
		YMONTH-=1
		YDAY=30
	else:
		YDAY-=1
	STARTTIME= wikicount.fnStartTimer()
	STRINGDATE=str(YEAR)+"_"+"%02d" % (MONTH,)+"_"+"%02d" % (DAY,)
	YSTRINGDATE=str(YEAR)+"_"+"%02d" % (YMONTH,)+"_"+"%02d" % (YDAY,)
	print STRINGDATE, YSTRINGDATE
	conn=Connection()
	db=conn.wc
	LANGUAGES= wikicount.getLanguageList()
	for lang in LANGUAGES:
		RECEVALS=0
		RECEXCEPTS=0
		DBOPS=0	
		MOVERS=[]
		hdTABLE=str(lang)+"_hitsdaily"	
		outTABLE=str(lang)+"_threehour"
		print hdTABLE
		DBOPS+=1
		RECSET=db[hdTABLE].find({STRINGDATE:{"$gt":10}})
		print RECSET.count()
		for RS in RECSET:
			try:
				rec=(RS[STRINGDATE]-RS[YSTRINGDATE],RS['_id'],RS['title'])
				MOVERS.append(rec)
				RECEVALS+=1
			except KeyError:
				rec=(RS[STRINGDATE],RS['_id'],RS['title'])
				MOVERS.append(rec)
				RECEVALS+=1
			except:
				RECEXCEPTS+=1
				continue
		SMOVERS=sorting.QuickSortListArray(MOVERS)
		db[outTABLE].remove()
		for a in range(1,100):
			try:
		                rec={'place':a,'title':SMOVERS[-a][2],'rollavg':SMOVERS[-a][0],'id':SMOVERS[-a][1]}
				db[outTABLE].insert(rec)
				DBOPS+=1
			except:
				continue

		syslog.syslog("[movers.py][main] "+str(lang)+" language done. DBOPS: "+str(DBOPS)+" Records sorted: "+str(len(SMOVERS))+" out of "+str(RECEVALS)+" initial recs.  Exceptions encountered: "+str(RECEXCEPTS))
	return

syslog.syslog("[movers.py][main] Start")
p98_todaysmovers()
syslog.syslog("[mover.py][main] Done")
