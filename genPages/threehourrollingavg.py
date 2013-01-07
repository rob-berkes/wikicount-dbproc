from pymongo import Connection
from functions import wikicount
from numpy import sqrt,mean,array
from operator import itemgetter

conn=Connection()
db=conn.wc

ALLRES=db.hitshourly.find({'16':{'$gt':15}}).sort('16',-1).limit(50000)

hourlies=[]
for item in ALLRES:
	title,utitle=wikicount.MapQuery_FindName(item['_id'])
	try:
		b1=item['14']
	except KeyError:
		b1=0
	try:
		b2=item['15']
	except KeyError:
		b2=0
	try:
		b3=item['16']
	except KeyError:
		b3=0
	
	rollingavg=mean(array([b1,b2,b3]))

	rec={'title':title,'rollavg':int(rollingavg)}
	hourlies.append(rec)

#for w in sorted(hourlies,key=itemgetter('rollavg'),reverse=True):
#	if w['title']:
#		print w['title'],w['rollavg']
z=1
for w in sorted(hourlies,key=itemgetter('rollavg'),reverse=True):
	if z < 101:
		rec={'place':z,'title':w['title'],'rollavg':w['rollavg']}
		db.threehour.insert(rec)
		z+=1
