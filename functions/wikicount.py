from pymongo import Connection 
import string
import urllib2
from datetime import date
import datetime
import time
from wsgiref.handlers import format_date_time
import syslog
import os 

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
def MapQuery_FindImageName(id):
        QUERY={'id':id}
        MAPQ=db.image.find_one({'_id':id})
        title=''
        utitle=''
        title=MAPQ['title']
        s_title=string.replace(title,'_',' ')
        t_title=s_title.encode('utf-8')
        utitle=urllib2.unquote(t_title)
        return title, utitle
def fnIsPrevJobDone(CURJOBNAME):
	DAY,MONTH,YEAR,HOUR,expiretime=fnReturnTimes()
	HOUR=minusHour(int(HOUR))
	HOUR=adjustHour(int(HOUR))
	if CURJOBNAME=='p0_dl':
		STATUSQUERY=db.logSystem.find_one({'table':'p0_dl'})
		CSTATUSQ=db.logSystem.find_one({'table':'p1_split'})
		if STATUSQUERY['mesg']==int(HOUR) and CSTATUSQ['mesg']==int(HOUR):
			syslog.syslog('Job already run, cron job NOT starting')
		elif STATUSQUERY['mesg']==int(HOUR):
			syslog.syslog('Job not started yet, returning True')
			return True
	elif CURJOBNAME=='p1_split':
		STATUSQUERY=db.logSystem.find_one({'table':'p1_split'})
		CSTATUSQ=db.logSystem.find_one({'table':'p2_filter'})
		if STATUSQUERY['mesg']==int(HOUR) and CSTATUSQ['mesg']==int(HOUR):
			syslog.syslog('Job already run, cron job NOT starting')
		elif STATUSQUERY['mesg']==int(HOUR):
			syslog.syslog('Job not started yet, returning True')
			return True
	elif CURJOBNAME=='p2_filter':
		STATUSQUERY=db.logSystem.find_one({'table':'p2_filter'})
		CSTATUSQ=db.logSystem.find_one({'table':'p2x_move_to_action'})
		if STATUSQUERY['mesg']==int(HOUR) and CSTATUSQ['mesg']==int(HOUR):
			syslog.syslog('Job already run, cron job NOT starting')
		elif STATUSQUERY['mesg']==int(HOUR):
			syslog.syslog('Job not started yet, returning True')
			return True
	elif CURJOBNAME=='p2x_move_to_action':
		STATUSQUERY=db.logSystem.find_one({'table':'p2x_move_to_action'})
		CSTATUSQ=db.logSystem.find_one({'table':'p3_add_to_db'})
		if STATUSQUERY['mesg']==int(HOUR) and CSTATUSQ['mesg']==int(HOUR):
			syslog.syslog('Job already run, cron job NOT starting')
		elif STATUSQUERY['mesg']==int(HOUR):
			syslog.syslog('Job not started yet, returning True')
			return True
	return False
def fnLaunchNextJob(CURJOBNAME):
	if CURJOBNAME=='p0_dl' and fnIsPrevJobDone(CURJOBNAME):
		syslog.syslog('Launching split proc...p1_split')
		os.system('/usr/bin/python /home/ec2-user/Python/p1_split.py') 		
	elif CURJOBNAME=='p1_split' and fnIsPrevJobDone(CURJOBNAME):
		syslog.syslog('Launching filter proc...p2_filter')
		os.system('/usr/bin/python /home/ec2-user/Python/p2_filter.py') 		
	elif CURJOBNAME=='p2_filter' and fnIsPrevJobDone(CURJOBNAME):
		syslog.syslog('Launching move to action proc...p2x_move_to_action')
		os.system('/usr/bin/python /home/ec2-user/Python/p2x_move_to_action.py') 		
	elif CURJOBNAME=='p2x_move_to_action' and fnIsPrevJobDone(CURJOBNAME):
		syslog.syslog('Launching add to db proc...p3_add_to_db')
		os.system('/usr/bin/python /home/ec2-user/Python/p3_add_to_db.py') 		

	return
def fnGetStatusMsg(COLLCHECK):
	RECORD=db.logSystem.find_one({table:COLLCHECK})
	
	return RECORD['status']
def fnStartTimer():
	return time.time()
def fnEndTimerCalcRuntime(a):
	b=time.time()
	c=b-a
	d=round(c,3)
	return d
def fnSetStatusMsg(COLLCHECK,msgNum):
	DAY,MONTH,YEAR,HOUR,expiretime=fnReturnTimes()
	HOUR=minusHour(int(HOUR))
	HOUR=adjustHour(int(HOUR))
	QREC={'table':COLLCHECK}
	if msgNum==0:
		REC={'table':COLLCHECK,'mesg':'NOT Done'}
		db.logSystem.remove(QREC)
		db.logSystem.insert(REC)
	elif msgNum==1:
		REC={'table':COLLCHECK,'mesg':'Done'}
		db.logSystem.remove(QREC)
		db.logSystem.insert(REC)
	elif msgNum==3:
		REC={'table':COLLCHECK,'mesg':HOUR}
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
