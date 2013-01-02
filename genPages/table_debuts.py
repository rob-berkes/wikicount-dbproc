#!/usr/bin/python
import urllib2
from pymongo import Connection
from datetime import date
import string 

TODAY=date.today()
DAY=TODAY.day
MONTH=TODAY.month
YEAR=TODAY.year
conn=Connection()
db=conn.wc
QUERY=db.tophits.find({'d':int(DAY),'m':int(MONTH),'y':int(YEAR)}).sort('place',1).skip(1000).limit(1000)
RECPERTABSET=50
TOTALNEW=0
send_list=[]
cit=0
hit=0
for item in QUERY:
    cit+=1
    print 'item '+str(cit)+'\n'	
    COUNT=0
    TITLE=''
    YQUERY=db.tophits.find({'d':int(DAY-1),'m':MONTH,'y':YEAR,'id':item['id']})
    for row in YQUERY:
         COUNT+=1
    if TOTALNEW>(RECPERTABSET-1):
         break
    if COUNT > 100000:
         break
    if COUNT==0 and TOTALNEW<RECPERTABSET:
	 hit+=1
	 print '*******HIT*****'+str(hit)+'\n'
         TOTALNEW+=1
         NQUERY=db.map.find({'_id':str(item['id'])})
         for it in NQUERY:
                   title=it['title']
                   s_title=string.replace(title,'_',' ')
                   s_title=string.replace(s_title,'/','')
                   t_title=s_title.encode('utf-8')
                   utitle=urllib2.unquote(t_title)
	 print 'inserting record'
         rec={'title':utitle,'place':item['place'],'linktitle':title.encode('utf-8')}
         db.topdebuts.insert(rec)

