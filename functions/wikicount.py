from pymongo import Connection 
import string
import urllib2
from datetime import date
import datetime
import time
from wsgiref.handlers import format_date_time
import syslog
import os
import HTMLParser

_htmlparser=HTMLParser.HTMLParser()
unescape=_htmlparser.unescape 

conn=Connection()
db=conn.wc


class logRecord():
    def __init__(self,language,page,views,size):
        self.language=language
        self.page=page
        self.views=views
        self.bwidth=size



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
			utitle=unescape(utitle)
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
	return True
def fnStub1_fnIsPrevJobDone(CURJOBNAME):
	#may be unneeded now that new job scheduling in place, leave for now
       if CURJOBNAME=='p0_dl':
               STATUSQUERY=db.logSystem.find_one({'table':'p0_dl'})
               CSTATUSQ=db.logSystem.find_one({'table':'p1_split'})
               if STATUSQUERY['mesg']==int(HOUR) and CSTATUSQ['mesg']==int(HOUR):
                       syslog.syslog('Job p1_split already run, cron job NOT starting')
               elif STATUSQUERY['mesg']==int(HOUR):
                       return True
       elif CURJOBNAME=='p1_split':
               STATUSQUERY=db.logSystem.find_one({'table':'p1_split'})
               CSTATUSQ=db.logSystem.find_one({'table':'p2_filter'})
               if STATUSQUERY['mesg']==int(HOUR) and CSTATUSQ['mesg']==int(HOUR):
                       syslog.syslog('Job p2_filter already run, cron job NOT starting')
               elif STATUSQUERY['mesg']==int(HOUR):
                       return True
       elif CURJOBNAME=='p2_filter':
               STATUSQUERY=db.logSystem.find_one({'table':'p2_filter'})
               CSTATUSQ=db.logSystem.find_one({'table':'p2x_move_to_action'})
               if STATUSQUERY['mesg']==int(HOUR) and CSTATUSQ['mesg']==int(HOUR):
                       syslog.syslog('Job p2x_move already run, cron job NOT starting')
               elif STATUSQUERY['mesg']==int(HOUR):
                       return True
       elif CURJOBNAME=='p2x_move_to_action':
               STATUSQUERY=db.logSystem.find_one({'table':'p2x_move_to_action'})
               CSTATUSQ=db.logSystem.find_one({'table':'p3_add_to_db'})
               if STATUSQUERY['mesg']==int(HOUR) and CSTATUSQ['mesg']==int(HOUR):
                       syslog.syslog('Job p3_add already run, cron job NOT starting')
               elif STATUSQUERY['mesg']==int(HOUR):
                       return True
       elif CURJOBNAME=='p3_add_to_db':
               STATUSQUERY=db.logSystem.find_one({'table':'p3_add_to_db'})
               CSTATUSQ=db.logSystem.find_one({'table':'p3_image_add'})
               if STATUSQUERY['mesg']==int(HOUR) and CSTATUSQ['mesg']==int(HOUR):
                       syslog.syslog('Job p3_image already run, cron job NOT starting')
               elif STATUSQUERY['mesg']==int(HOUR):
                       return True
       elif CURJOBNAME=='p3_image_add':
               STATUSQUERY=db.logSystem.find_one({'table':'p3_image_add'})
               CSTATUSQ=db.logSystem.find_one({'table':'p3_category_add'})
               if STATUSQUERY['mesg']==int(HOUR) and CSTATUSQ['mesg']==int(HOUR):
                       syslog.syslog('Job p3_category already run, cron job NOT starting')
               elif STATUSQUERY['mesg']==int(HOUR):
                       return True
       elif CURJOBNAME=='p3_category_add':
               STATUSQUERY=db.logSystem.find_one({'table':'p3_category_add'})
               CSTATUSQ=db.logSystem.find_one({'table':'p70_export'})
               if STATUSQUERY['mesg']==int(HOUR) and CSTATUSQ['mesg']==int(HOUR):
                       syslog.syslog('Job p70_export already run, cron job NOT starting')
               elif STATUSQUERY['mesg']==int(HOUR):
                       return True
       elif CURJOBNAME=='p70_export':
               STATUSQUERY=db.logSystem.find_one({'table':'p70_export'})
               CSTATUSQ=db.logSystem.find_one({'table':'p90_remove_noise'})
               if STATUSQUERY['mesg']==int(HOUR) and CSTATUSQ['mesg']==int(HOUR):
                       syslog.syslog('Job p90_remove_noise already run, cron job NOT starting')
               elif STATUSQUERY['mesg']==int(HOUR):
                       return True
       elif CURJOBNAME=='p90_remove_noise':
               STATUSQUERY=db.logSystem.find_one({'table':'p90_remove_noise'})
               CSTATUSQ=db.logSystem.find_one({'table':'sortMongoHD'})
               if STATUSQUERY['mesg']==int(HOUR) and CSTATUSQ['mesg']==int(HOUR):
                       syslog.syslog('Job sortMongoHD already run, cron job NOT starting')
               elif STATUSQUERY['mesg']==int(HOUR):
                       return True
       elif CURJOBNAME=='sortMongoHD':
               STATUSQUERY=db.logSystem.find_one({'table':'sortMongoHD'})
               CSTATUSQ=db.logSystem.find_one({'table':'tophits'})
               if STATUSQUERY['mesg']==int(HOUR) and CSTATUSQ['mesg']==int(HOUR):
                       syslog.syslog('Job tophits already run, cron job NOT starting')
               elif STATUSQUERY['mesg']==int(HOUR):
                       return True
       elif CURJOBNAME=='tophits':
       #       STATUSQUERY=db.logSystem.find_one({'table':'tophits'})
       #       CSTATUSQ=db.logSystem.find_one({'table':'threehrrollingavg'})
       #       if STATUSQUERY['mesg']==int(HOUR) and CSTATUSQ['mesg']==int(HOUR):
       #               syslog.syslog('Job threehrrollingavg already run, cron job NOT starting')
       #       elif STATUSQUERY['mesg']==int(HOUR):
               return True
       elif CURJOBNAME=='threehrrollingavg':
               STATUSQUERY=db.logSystem.find_one({'table':'threehrrollingavg'})
               CSTATUSQ=db.logSystem.find_one({'table':'fillTmpHot'})
               if STATUSQUERY['mesg']==int(HOUR) and CSTATUSQ['mesg']==int(HOUR):
                       syslog.syslog('Job fillTmpHot already run, cron job NOT starting')
               elif STATUSQUERY['mesg']==int(HOUR):
                       return True
       elif CURJOBNAME=='fillTmpHot':
               STATUSQUERY=db.logSystem.find_one({'table':'fillTmpHot'})
               CSTATUSQ=db.logSystem.find_one({'table':'populate_trending'})
               if STATUSQUERY['mesg']==int(HOUR) and CSTATUSQ['mesg']==int(HOUR):
                       syslog.syslog('Job populate_trending already run, cron job NOT starting')
               elif STATUSQUERY['mesg']==int(HOUR):
                       return True
       elif CURJOBNAME=='populate_trending':
               STATUSQUERY=db.logSystem.find_one({'table':'populate_trending'})
               CSTATUSQ=db.logSystem.find_one({'table':'populate_debuts'})
               if STATUSQUERY['mesg']==int(HOUR) and CSTATUSQ['mesg']==int(HOUR):
                       syslog.syslog('Job populate_debuts already run, cron job NOT starting')
               elif STATUSQUERY['mesg']==int(HOUR):
                       return True
       elif CURJOBNAME=='populate_debuts':
               STATUSQUERY=db.logSystem.find_one({'table':'populate_debuts'})
               CSTATUSQ=db.logSystem.find_one({'table':'populate_cold'})
               if STATUSQUERY['mesg']==int(HOUR) and CSTATUSQ['mesg']==int(HOUR):
                       syslog.syslog('Job pop_cold already run, cron job NOT starting')
               elif STATUSQUERY['mesg']==int(HOUR):
                       return True
       elif CURJOBNAME=='populate_cold':
               STATUSQUERY=db.logSystem.find_one({'table':'populate_cold'})
               CSTATUSQ=db.logSystem.find_one({'table':'populate_category'})
               if STATUSQUERY['mesg']==int(HOUR) and CSTATUSQ['mesg']==int(HOUR):
                       syslog.syslog('Job pop_cat already run, cron job NOT starting')
               elif STATUSQUERY['mesg']==int(HOUR):
                       return True
       elif CURJOBNAME=='populate_category':
               STATUSQUERY=db.logSystem.find_one({'table':'populate_category'})
               CSTATUSQ=db.logSystem.find_one({'table':'populate_image'})
               if STATUSQUERY['mesg']==int(HOUR) and CSTATUSQ['mesg']==int(HOUR):
                       syslog.syslog('Job pop_image already run, cron job NOT starting')
               elif STATUSQUERY['mesg']==int(HOUR):
                       return True
       elif CURJOBNAME=='populate_image':
               return True
       return False
def getLanguageList():
	LList=['ru','ja','zh']
	return LList

def fnLaunchNextJob(CURJOBNAME):
       if CURJOBNAME=='p0_dl' and fnIsPrevJobDone(CURJOBNAME):
               syslog.syslog('Launching split proc...p1_split')
               os.system('/usr/bin/python /home/ec2-user/Python/p1_split.py')          
       elif CURJOBNAME=='p1_split' and fnIsPrevJobDone(CURJOBNAME):
               syslog.syslog('Launching filter proc...p2_filter')
               os.system('/usr/bin/python /home/ec2-user/Python/p2_filter.py')                 
       elif CURJOBNAME=='p2_filter' and fnIsPrevJobDone(CURJOBNAME):
               syslog.syslog('Launching Russian filter...p2_ru_filter')
               os.system('/usr/bin/python /home/ec2-user/Python/p2_ru_filter.py')                
       elif CURJOBNAME=='p2_ru_filter' and fnIsPrevJobDone(CURJOBNAME):
               syslog.syslog('Launching move to action proc...p2x_move_to_action')
               os.system('/usr/bin/python /home/ec2-user/Python/p2x_move_to_action.py')                
       elif CURJOBNAME=='p2x_move_to_action' and fnIsPrevJobDone(CURJOBNAME):
               syslog.syslog('Launching add to db proc...p3_add_to_db')
               os.system('/usr/bin/python /home/ec2-user/Python/p3_add_to_db.py')              
       elif CURJOBNAME=='p3_add_to_db' and fnIsPrevJobDone(CURJOBNAME):
               syslog.syslog('Launching Image add to db...p3_image_add')
               os.system('/usr/bin/python /home/ec2-user/Python/p3_image_add_to_db.py')                
       elif CURJOBNAME=='p3_image_add' and fnIsPrevJobDone(CURJOBNAME):
               syslog.syslog('Launching Category add to db ...p3_category_add')
               os.system('/usr/bin/python /home/ec2-user/Python/p3_category_add_to_db.py')             
       elif CURJOBNAME=='p3_category_add' and fnIsPrevJobDone(CURJOBNAME):
               syslog.syslog('Launching p70 Export to csv file ...p70_export')
               os.system('/usr/bin/python /home/ec2-user/Python/p70_export.py')                
       elif CURJOBNAME=='p70_export' and fnIsPrevJobDone(CURJOBNAME):
               syslog.syslog('Launching p90 noise removal script ...p90_remove_noise')
               os.system('/usr/bin/python /home/ec2-user/Python/p90_remove_noise.py')          
       elif CURJOBNAME=='p90_remove_noise' and fnIsPrevJobDone(CURJOBNAME):
               syslog.syslog('Launching Mongo CSV Sort script ...sortMongoHD')
               os.system('/usr/bin/python /home/ec2-user/Python/sort_MongoHD.py')              
       elif CURJOBNAME=='sortMongoHD' and fnIsPrevJobDone(CURJOBNAME):
               syslog.syslog('Launching Tophits script ...tophits')
               os.system('/usr/bin/python /home/ec2-user/Python/tophits.py')           
       elif CURJOBNAME=='tophits' and fnIsPrevJobDone(CURJOBNAME):
               syslog.syslog('Launching ThreeHourRollingAvg script ...threehrrollingavg')
               os.system('/usr/bin/python /home/ec2-user/Python/genPages/threehourrollingavg.py')              
       elif CURJOBNAME=='threehrrollingavg' and fnIsPrevJobDone(CURJOBNAME):
               syslog.syslog('Launching prepop_filltmpHot script ...fillTmpHot')
               os.system('/usr/bin/python /home/ec2-user/Python/genPages/prepop_filltmpHot.py')                
       elif CURJOBNAME=='fillTmpHot' and fnIsPrevJobDone(CURJOBNAME):
               syslog.syslog('Launching populate_trending script ...populate_trending')
               os.system('/usr/bin/python /home/ec2-user/Python/genPages/populate_trending.py')                
       elif CURJOBNAME=='populate_trending' and fnIsPrevJobDone(CURJOBNAME):
               syslog.syslog('Launching populate_debuts script ...populate_debuts')
               os.system('/usr/bin/python /home/ec2-user/Python/genPages/populate_debuts.py')          
       elif CURJOBNAME=='populate_debuts' and fnIsPrevJobDone(CURJOBNAME):
               syslog.syslog('Launching populate_cold script ...populate_cold')
               os.system('/usr/bin/python /home/ec2-user/Python/genPages/populate_cold.py')            
       elif CURJOBNAME=='populate_cold' and fnIsPrevJobDone(CURJOBNAME):
               syslog.syslog('Launching populate_category script ...populate_category')
               os.system('/usr/bin/python /home/ec2-user/Python/genPages/populate_category.py')                
       elif CURJOBNAME=='populate_category' and fnIsPrevJobDone(CURJOBNAME):
               syslog.syslog('Launching populate_image script ...populate_image')
               os.system('/usr/bin/python /home/ec2-user/Python/genPages/populate_image.py')           
       elif CURJOBNAME=='populate_image' and fnIsPrevJobDone(CURJOBNAME):
               syslog.syslog('All Done with adding info to databae!')

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
