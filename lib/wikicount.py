#coding: utf-8
from pymongo import Connection 
import string
import urllib2
from datetime import date
import datetime
import time
from wsgiref.handlers import format_date_time
import syslog
import HTMLParser

_htmlparser=HTMLParser.HTMLParser()
unescape=_htmlparser.unescape 

conn=Connection()
db=conn.wc
LList=['en','hy','ru','ja','zh','es','fr','pl','pt','it','de','ro','eo','hr','ar','la','sw','af','simple',
           'en.b','en.q','en.s','en.d','en.voy','fr.d','fr.b','sv','ja.b','it.b','de.b','commons.m','it.q',
           'pl.q','ru.q','zh.q']

fnStrFmtDate = lambda DVAR :'%02d' %(DVAR,)
fnStartTimer = lambda : time.time
fnEndTimerCalcRuntime = lambda a, b=time.time(): round(b-a,5)
fnFormatHour = lambda HOUR : '%02d' %(int(HOUR),)
fnReturnTimeString = lambda DAY, MONTH, YEAR : str('%02d'%(int(DAY,)))+"_"+str('%02d'%(int(MONTH,)))+"_"+str('%02d'%(int(YEAR,)))


class logRecord():
    def __init__(self,language,page,views,size):
        self.language=language
        self.page=page
        self.views=views
        self.bwidth=size

fnGetMonthName = lambda : datetime.datetime.now().strftime("%B")

def MapQuery_FindName(id):
        MAPQ=db.hitsdaily.find({'_id':id})
        title=''
        utitle=''
        for name in MAPQ:
            title=name['title']
            s_title=string.replace(title,'_',' ')
            t_title=s_title.encode('utf-8')
            utitle=urllib2.unquote(t_title)
            utitle=unescape(utitle)
        return title, utitle

def MapQuery_FindCategory(id):
        MAPQ=db.category.find_one({'_id':id})
        title=MAPQ['title']
        s_title=string.replace(title,'_',' ')
        t_title=s_title.encode('utf-8')
        utitle=urllib2.unquote(t_title)
        return title, utitle
def MapQuery_FindImageName(id):
        MAPQ=db.image.find_one({'_id':id})
        title=MAPQ['title']
        s_title=string.replace(title,'_',' ')
        t_title=s_title.encode('utf-8')
        utitle=urllib2.unquote(t_title)
        return title, utitle
def getLanguageList():
	LList=['en','hy','ru','ja','zh','es','fr','pl','pt','it','de','ro','eo','hr','ar','la','sw','af','simple',
           'en.b','en.q','en.s','en.d','en.voy','fr.d','fr.b','sv','ja.b','it.b','de.b','commons.m','it.q',
           'pl.q','ru.q','zh.q']


	return LList


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



def FormatName(title):
        s_title=string.replace(title,'_',' ')
        t_title=s_title.encode('utf-8')
        utitle=urllib2.unquote(t_title)
	try:
		utitle=utitle.decode('utf-8')
	except:
		utitle=title
        return title, utitle


def adjustHour(HOUR):
	if HOUR==-1:
		HOUR=23
	elif HOUR==-2:
		HOUR=22
	return HOUR

def minusHour(HOUR):
	HOUR-=1
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
	c=HOUR-3
	if a==-1:
		a=23
	if b==-1:
		b=23
	elif b==-2:
		b=22
	if c<0:
		c+=24
	return a,b,c




def toSyslog(msg):
    syslog.syslog(msg)

def PreviousDay(YEAR,MONTH,DAY):
    yd=DAY-1
    ym=MONTH
    yy=YEAR
    if yd==0:
        yd=30
        ym=MONTH-1
        if ym==0:
            ym=12
            yy=YEAR-1
    return yy,ym,yd
