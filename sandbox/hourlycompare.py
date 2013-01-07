from pymongo import Connection
from functions import wikicount
from numpy import sqrt
from operator import itemgetter

conn=Connection()
db=conn.wc

TITLEQ=db.map.find_one({"_id":"cacf3ccaa4ad086d3787c60d47655ea6f2f444f0"})
RESULT=db.hitshourly.find_one({"_id":"cacf3ccaa4ad086d3787c60d47655ea6f2f444f0"})

print TITLEQ['title']
x1=10
y1=RESULT['10']
x2=11
y2=RESULT['11']
x3=12
y3=RESULT['12']
print '10 : '+str(y1)
print '11 : '+str(y2)
print '12 : '+str(y3)




ALLRES=db.hitshourly.find({"10":{"$gt":5}}).limit(20000)

hourlies=[]
for item in ALLRES:
	title,utitle=wikicount.MapQuery_FindName(item['_id'])
	a1=10
	b1=item['10']
	a2=11
	try:
		b2=item['11']
	except KeyError:
		b2=250000
	a3=12
	try:
		b3=item['12']
	except KeyError:
		b3=250000
	
	day1=sqrt((b1-y1)**2+(a1-x1)**2)
	day2=sqrt((b2-y2)**2+(a2-x2)**2)
	day3=sqrt((b3-y3)**2+(a3-x3)**2)

	sum=day1+day2+day3
	rec={'title':title,'sum':int(sum)}
	hourlies.append(rec)

for w in sorted(hourlies,key=itemgetter('sum')):
	if w['title']:
		print w['title'],w['sum']
