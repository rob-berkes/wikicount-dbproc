#/usr/bin/python
from pymongo import Connection
from datetime import date
import time 
def HTMLHeader(FILE):
	FILE.write("<HTML><HEAD>New and Hot 25 Pages</HEAD>")
	FILE.write("<TITLE>25 WP pages new to the top 250,000</TITLE>\n")
	FILE.write("<BODY><CENTER>For "+time.strftime("%D")+".</CENTER> ")
	FILE.write("<TABLE border='0'>")
def HTMLFooter(FILE):
	FILE.write("</TABLE></BODY></HTML>")

TODAY=date.today()
DAY=TODAY.day
MONTH=TODAY.month
YEAR=TODAY.year
HTMLDIR="/home/ec2-user/Python/genPages/html/"
HTMLFILE="newPages_"+str(MONTH)+"_"+str(DAY)+"_"+str(YEAR)+".html"
OUTFILE=HTMLDIR+HTMLFILE
OFILE=open(OUTFILE,'w')
HTMLHeader(OFILE)
OFILE.write('<table>')
conn=Connection()
db=conn.wc
QUERY=db.tophits.find({'d':int(DAY)}).sort('place',1)  #limit for debugging only
RC=0
COUNT=0
TOTALNEW=0
for item in QUERY:
	COUNT=0
	TITLE=''
	YQUERY=db.tophits.find({'d':int(DAY)-1,'m':MONTH,'y':YEAR,'id':item['id']})
	for row in YQUERY:
		COUNT+=1
	if TOTALNEW>24:
		break
	if COUNT==0 and TOTALNEW<25:
		TOTALNEW+=1	
		NQUERY=db.map.find({'_id':str(item['id'])})
		for it in NQUERY:
			TITLE=it['title']
		OFILEstr=('<tr><td>'+str(TOTALNEW)+'</td><td>'+str(TITLE)+'</td><td>Place:'+str(item['place'])+'</td></tr>')
		OFILE.write(OFILEstr)	
OFILE.write('</table>')
HTMLFooter(OFILE)
