from pymongo import Connection 
import string
import urllib2
from datetime import date
import datetime
import time
from wsgiref.handlers import format_date_time

conn=Connection()
db=conn.wc

def MapQuery_FindName(id):
        QUERY={'id':id}
        MAPQ=db.hitsdaily.find({'_id':id})
        title=''
        utitle=''
        for name in MAPQ:
                        title=name['title']
                        s_title=string.replace(title,'_',' ')
                        t_title=s_title.encode('utf-8')
                        utitle=urllib2.unquote(t_title)
        return title, utitle
def MapQuery_FindCategory(id):
        QUERY={'id':id}
        MAPQ=db.category.find_one({'_id':id})
        title=''
        utitle=''
        title=MAPQ['title']
        s_title=string.replace(title,'_',' ')
        t_title=s_title.encode('utf-8')
        utitle=urllib2.unquote(t_title)
        return title, utitle
def fnGetStatusMsg(COLLCHECK):
	RECORD=db.logSystem.find_one({table:COLLCHECK})
	
	return RECORD['status']
def fnSetStatusMsg(COLLCHECK,msgNum):
	QREC={'table':COLLCHECK}
	if msgNum==0:
		REC={'table':COLLCHECK,'mesg':'NOT Done'}
		db.logSystem.remove(QREC)
		db.logSystem.insert(REC)
	elif msgNum==1:
		REC={'table':COLLCHECK,'mesg':'Done'}
		db.logSystem.remove(QREC)
		db.logSystem.insert(REC)
		
	return
def fnWaitForStatus(COLLCHECK):
	QREC={'table':COLLCHECK}
	REC=db.logSystem.find_one(QREC)
	if REC['mesg'] == 'NOT Done':
		time.sleep(5)
	return
def fnGetMonthName():
	return datetime.datetime.now().strftime("%B")

def fnReturnTimes():
        TODAY=date.today()
        YEAR=TODAY.year
        DAY=TODAY.day
        MONTH=TODAY.month
        HOUR=time.strftime('%H')
        now=datetime.datetime.now()
        half=now+datetime.timedelta(minutes=45)
        stamp=time.mktime(half.timetuple())
        expiretime=format_date_time(stamp)
	if int(HOUR) < 8:
		DAY-=1
        if DAY==0:
           DAY=30
           MONTH-=1
        if MONTH==0:
           DAY=31
           MONTH=12
           YEAR-=1
        return DAY, MONTH, YEAR,HOUR, expiretime


def fnFormatTimes(DAY,MONTH,HOUR):
	HOUR='%02d' % (int(HOUR),)
	DAY='%02d' % (int(DAY),)
	MONTH='%02d' % (int(MONTH),)
	return DAY,MONTH,HOUR
def fnReturnTimeString(DAY,MONTH,YEAR):
	YEAR='%02d' % (int(YEAR),)
	DAY='%02d' % (int(DAY),)
	MONTH='%02d' % (int(MONTH),)
	return str(YEAR)+"_"+str(MONTH)+"_"+str(DAY)

def FormatName(title):
        s_title=string.replace(title,'_',' ')
        t_title=s_title.encode('utf-8')
        utitle=urllib2.unquote(t_title)
#	try:
#		utitle=utitle.decode('utf-8')
#	except:
#		utitle=utitle.encode('utf-8')
        return title, utitle

def adjustHour(HOUR):
	if HOUR==-1:
		HOUR=23
	elif HOUR==-2:
		HOUR=22
	return HOUR

def minusHour(HOUR):
	HOUR-=7
	if HOUR==-1:
		HOUR=23
	elif HOUR==-2:
		HOUR=22
	elif HOUR==-3:
		HOUR=21
	elif HOUR==-4:
		HOUR=20
	elif HOUR==-5:
		HOUR=19
	elif HOUR==-6:
		HOUR=18
	elif HOUR==-7:
		HOUR=17
	return HOUR

def fnReturnLastThreeHours(HOUR):
	a=HOUR-1
	b=HOUR-2
	if a==-1:
		a=23
	if b==-1:
		b=23
	elif b==-2:
		b=22
	return HOUR,a,b
def fnStrFmtDate(DVAR):
	NEWVAR='%02d' % (DVAR,)	
	return NEWVAR
