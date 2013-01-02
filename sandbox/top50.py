#/usr/bin/python
from pymongo import Connection
from datetime import date
import time 

def HTMLHeader(FILE):
	FILE.write("<HTML><HEAD>Top 50 Pages</HEAD>")
	FILE.write("<TITLE>25 most viewed WP pages</TITLE>\n")
	FILE.write("<BODY><CENTER>For "+time.strftime("%D")+".</CENTER> ")
	FILE.write("<TABLE border='0'>")
def HTMLFooter(FILE):
	FILE.write("</TABLE></BODY></HTML>")
TODAY=date.today()
DAY=TODAY.day
MONTH=TODAY.month
YEAR=TODAY.year
HTMLDIR="/home/ec2-user/Python/genPages/html/"
HTMLFILE=HTMLDIR+"/top50_"+str(YEAR)+"_"+str(MONTH)+"_"+str(DAY)+".html"
conn=Connection()
db=conn.wc
QUERY=db.tophits.find({'d':DAY,'m':MONTH,'y':YEAR}).sort('Hits',-1).limit(50)

OFILE=open(HTMLFILE,"w")
HTMLHeader(OFILE)

PLACE=1
for line in QUERY:
	HASH=line['id']
	SEARCHQ={'_id':str(HASH)}
	SEARCH=db.map.find(SEARCHQ)
	for item in SEARCH:
		OFILE.write("<tr><td>"+str(PLACE)+"</td><td>"+unicode(item['title']+"</td></tr>"))
	PLACE+=1

HTMLFooter(OFILE)
OFILE.close()
