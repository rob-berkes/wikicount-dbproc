#!/usr/bin/python
import string 
import urllib2
import random
from pymongo import Connection
from datetime import date
from datetime import time
import datetime
import subprocess
conn=Connection()
db=conn.wc
TODAY=date.today()
DAY=TODAY.day
MONTH=TODAY.month
YEAR=TODAY.year
MONTHNAME=datetime.datetime.now().strftime("%B")
thCN='tophits'+str(YEAR)+MONTHNAME
dbCN='proddebuts'+str(YEAR)+str(MONTHNAME)
DAILYPAGERESULTS=db.command({'distinct':thCN,'key':'d','query':{'m':int(MONTH)}})

def latestnews():
        ARTICLELIMIT=5
        latest_news_list = db.news.find().sort('date',-1).limit(ARTICLELIMIT)
        return latest_news_list


def Query_NewsFind(FINDQUERY,notedate,notes):
        findresults=db.news.find(FINDQUERY)
        for a in findresults:
                notedate=a['date']
                notes=a['text']

        return
def MapQuery_FindName(id):
        QUERY={'id':id}
        MAPQ=db.hitsdaily.find({'_id':id})
        latest_news_list = latestnews()
        title=''
        utitle=''
        for name in MAPQ:
                        title=name['title']
                        s_title=string.replace(title,'_',' ')
                        t_title=s_title.encode('utf-8')
                        utitle=urllib2.unquote(t_title)


        return title, utitle
def FormatName(title):
        s_title=string.replace(title,'_',' ')
        t_title=s_title.encode('utf-8')
        utitle=urllib2.unquote(t_title)
        return title, utitle

def returnHourString(hour):
	HOUR='%02d' % (hour,)
	return HOUR
def GenHourlyGraph(id):
	RESULT1=db.hitshourly.find_one({"_id":str(id)})
	OFILE=open('output.log','w')
	try:
		for i in range(0,24):
			HOUR=returnHourString(i)	
			try:
				OFILE.write(str(HOUR)+' '+str(RESULT1[HOUR])+'\n')
			except TypeError:
				pass 
	except KeyError:
		pass
	OFILE.close()
	subprocess.call(["gnuplot","gnuplot.plot"])
	OUTFILENAME='/tmp/django/wikicount/static/images/'+str(id)+'.png'
	SFILE='/tmp/django/wikicount/introduction.png'
	subprocess.Popen("mv "+str(SFILE)+" "+str(OUTFILENAME),shell=True)
	return
def GenInfoPage(id):
	GenHourlyGraph(id)
	MONTHNAME=datetime.datetime.now().strftime("%B")
	thCN='tophits'+str(YEAR)+MONTHNAME
	QUERY={'id':id}
	Q50K={'id':id,'place':{'$lt':50001}}
	Q5K={'id':id,'place':{'$lt':5001}}
	Q500={'id':id,'place':{'$lt':501}}
	Q50={'id':id,'place':{'$lt':51}}
	FINDQ=db[thCN].find(QUERY)
	DFINDQ=db.tophits.find(QUERY)
	D50KFINDQ=db.tophits.find(Q50K)
	D5KFINDQ=db.tophits.find(Q5K)
	D500FINDQ=db.tophits.find(Q500)
	D50FINDQ=db.tophits.find(Q50)
	INFOVIEW_KEY='infoview_'+str(id)
	INFOVIEWLT_KEY='infoviewlt_'+str(id)
	INFOVIEWLT5K_KEY='infoviewlt5k_'+str(id)
	INFOVIEWLT500_KEY='infoviewlt500_'+str(id)
	INFOVIEWLT50_KEY='infoviewlt50_'+str(id)
	send_list=[]
	info_lt50k_list=[]
	info_lt5k_list=[]
	info_lt500_list=[]
	info_lt50_list=[]
        
	OFILE250K=open("/tmp/t250k.log","w")	
	for result in DFINDQ:
	        rec={'d':str(result['d']),'m':str(result['m']),'y':str(result['y']),'place':str(result['place'])}
		OFILE250K.write(str(rec['y'])+'/'+str(rec['m'])+'/'+str(rec['d'])+' '+str(rec['place'])+'\n')
        	send_list.append(rec)
        for result in FINDQ:
	        rec={'d':str(result['d']),'m':str(result['m']),'y':str(result['y']),'place':str(result['place'])}
		OFILE250K.write(str(rec['y'])+'/'+str(rec['m'])+'/'+str(rec['d'])+' '+str(rec['place'])+'\n')
        	send_list.append(rec)
	OFILE250K.close()
	subprocess.call(["gnuplot","gnuplot.250k"])
	OUTFILENAME='/tmp/django/wikicount/static/images/t250k_'+str(id)+'.png'
	SFILE='/tmp/t250k.png'
	subprocess.Popen("mv "+str(SFILE)+" "+str(OUTFILENAME),shell=True)
	
	OFILE50K=open("/tmp/t50k.log","w")	
	LT50KQ=db[thCN].find(Q50K)
        for result in D50KFINDQ:
	        rec={'d':str(result['d']),'m':str(result['m']),'y':str(result['y']),'place':str(result['place'])}
		OFILE50K.write(str(rec['y'])+'/'+str(rec['m'])+'/'+str(rec['d'])+' '+str(rec['place'])+'\n')
        	info_lt50k_list.append(rec)
        for result in LT50KQ:
	        rec={'d':str(result['d']),'m':str(result['m']),'y':str(result['y']),'place':str(result['place'])}
		OFILE50K.write(str(rec['y'])+'/'+str(rec['m'])+'/'+str(rec['d'])+' '+str(rec['place'])+'\n')
        	info_lt50k_list.append(rec)
	OFILE50K.close()
	subprocess.call(["gnuplot","gnuplot.50k"])
	OUTFILENAME='/tmp/django/wikicount/static/images/t50k_'+str(id)+'.png'
	SFILE='/tmp/t50k.png'
	subprocess.Popen("mv "+str(SFILE)+" "+str(OUTFILENAME),shell=True)

	OFILE5K=open("/tmp/t5k.log","w")	
	LT5KQ=db[thCN].find(Q5K)
        for result in D5KFINDQ:
	        rec={'d':str(result['d']),'m':str(result['m']),'y':str(result['y']),'place':str(result['place'])}
		OFILE5K.write(str(rec['y'])+'/'+str(rec['m'])+'/'+str(rec['d'])+' '+str(rec['place'])+'\n')
        	info_lt5k_list.append(rec)
        for result in LT5KQ:
	        rec={'d':str(result['d']),'m':str(result['m']),'y':str(result['y']),'place':str(result['place'])}
		OFILE5K.write(str(rec['y'])+'/'+str(rec['m'])+'/'+str(rec['d'])+' '+str(rec['place'])+'\n')
        	info_lt5k_list.append(rec)
	OFILE5K.close()
	subprocess.call(["gnuplot","gnuplot.5k"])
	OUTFILENAME='/tmp/django/wikicount/static/images/t5k_'+str(id)+'.png'
	SFILE='/tmp/t5k.png'
	subprocess.Popen("mv "+str(SFILE)+" "+str(OUTFILENAME),shell=True)

	LT500=db[thCN].find(Q500)
	OFILE500=open("/tmp/top500.log","w")
        for result in D500FINDQ:
	        rec={'d':str(result['d']),'m':str(result['m']),'y':str(result['y']),'place':str(result['place'])}
		OFILE500.write(str(rec['y'])+'/'+str(rec['m'])+'/'+str(rec['d'])+' '+str(rec['place'])+'\n')
        	info_lt500_list.append(rec)
        for result in LT500:
	        rec={'d':str(result['d']),'m':str(result['m']),'y':str(result['y']),'place':str(result['place'])}
		OFILE500.write(str(rec['y'])+'/'+str(rec['m'])+'/'+str(rec['d'])+' '+str(rec['place'])+'\n')
        	info_lt500_list.append(rec)
	OFILE500.close()	
	subprocess.call(["gnuplot","gnuplot.500"])
	OUTFILENAME='/tmp/django/wikicount/static/images/t500_'+str(id)+'.png'
	SFILE='/tmp/top500.png'
	subprocess.Popen("mv "+str(SFILE)+" "+str(OUTFILENAME),shell=True)

	LT50=db[thCN].find(Q50)
	OFILE50=open("/tmp/top50.log","w")
        for result in D50FINDQ:
	        rec={'d':str(result['d']),'m':str(result['m']),'y':str(result['y']),'place':str(result['place'])}
		OFILE50.write(str(rec['y'])+'/'+str(rec['m'])+'/'+str(rec['d'])+' '+str(rec['place'])+'\n')
        	info_lt50_list.append(rec)
        for result in LT50:
	        rec={'d':str(result['d']),'m':str(result['m']),'y':str(result['y']),'place':str(result['place'])}
		OFILE50.write(str(rec['y'])+'/'+str(rec['m'])+'/'+str(rec['d'])+' '+str(rec['place'])+'\n')
        	info_lt50_list.append(rec)
	OFILE50.close()	
	subprocess.call(["gnuplot","gnuplot.50"])
	OUTFILENAME='/tmp/django/wikicount/static/images/t50_'+str(id)+'.png'
	SFILE='/tmp/top50.png'
	subprocess.Popen("mv "+str(SFILE)+" "+str(OUTFILENAME),shell=True)


	return



print 'now trending list query....'
send_list=[]   
DAY=date.today().day
MONTH=date.today().month
YEAR=date.today().year
#DAY-=1
print DAY,MONTH,YEAR 
TRENDING_LIST_QUERY=db.prodtrend.find({u'd':DAY,u'm':MONTH,u'y':YEAR}).sort('Hits',-1).limit(150)
#TRENDING_LIST_QUERY=db.prodtrend.find().sort('Hits',-1).limit(150)
print TRENDING_LIST_QUERY.count()
for p in TRENDING_LIST_QUERY:
	print p
	rec={'title':p['title'],'place':p['place'],'Hits':p['Hits'],'linktitle':p['linktitle'],'id':p['id']}
        send_list.append(rec)
	GenInfoPage(p['id'])

