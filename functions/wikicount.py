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
        MAPQ=db.map.find({'_id':id})
        title=''
        utitle=''
        for name in MAPQ:
                        title=name['title']
                        s_title=string.replace(title,'_',' ')
                        t_title=s_title.encode('utf-8')
                        utitle=urllib2.unquote(t_title)
        return title, utitle


def fnReturnTimes():
        TODAY=date.today()
        YEAR=TODAY.year
        DAY=TODAY.day
        MONTH=TODAY.month
        now=datetime.datetime.now()
        half=now+datetime.timedelta(minutes=45)
        stamp=time.mktime(half.timetuple())
        expiretime=format_date_time(stamp)
        if DAY==0:
           DAY=30
           MONTH-=1
        if MONTH==0:
           DAY=31
           MONTH=12
           YEAR-=1
        HOUR=time.strftime('%H')
        return DAY, MONTH, YEAR,HOUR, expiretime


def fnFormatTimes(DAY,MONTH,HOUR):
	HOUR='%02d' % (int(HOUR),)
	DAY='%02d' % (int(DAY),)
	MONTH='%02d' % (int(MONTH),)
	return DAY,MONTH,HOUR

def adjustHour(HOUR):
	if HOUR==-1:
		HOUR=23
	elif HOUR==-2:
		HOUR=22
	return HOUR
