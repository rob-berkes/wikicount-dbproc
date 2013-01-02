#/usr/bin/python
import time
from pymongo import Connection
from datetime import date
def HTMLHeader(FILE):
	FILE.write("<HTML><HEAD>Cold 25 Pages</HEAD>")
	FILE.write("<TITLE>25 most views lost</TITLE>\n")
	FILE.write("<BODY><CENTER>For "+time.strftime("%D")+".</CENTER> ")
	FILE.write("<TABLE border='0'>")
def HTMLFooter(FILE):
	FILE.write("</TABLE></BODY></HTML>")

TODAY=date.today()
DAY=TODAY.day
MONTH=TODAY.month
YEAR=TODAY.year
HTMLDIR="/home/ec2-user/Python/genPages/html/"
HTMLFILE=HTMLDIR+"/cold25_"+str(YEAR)+"_"+str(MONTH)+"_"+str(DAY)+".html"
OFILE=open(HTMLFILE,'w')

HTMLHeader(OFILE)

conn=Connection()
db=conn.wc

HOTRESULT=db.tmpHot.find().sort('delta',1).limit(25)
PLACECT=1
OFILE.write('<table>')
for item in HOTRESULT:
	TITLE="<null>"
        TRES=db.map.find({'_id':item['id']})
        for res in TRES:
                TITLE=res['title']
	OFILE.write('<tr><td>'+str(PLACECT)+'</td><td>'+str(TITLE)+'</td><td>'+str(item['delta'])+'</td></tr>')
        PLACECT+=1

HTMLFooter(OFILE)
OFILE.close()

