#/usr/bin/python
from pymongo import Connection
from datetime import date
import time 
import string
import urllib2


def HTMLHeader(FILE):
	FILE.write("<HTML><HEAD><link href=\"style.css\" rel=\"stylesheet\" type=\"text/css\"></HEAD>\n")
	FILE.write("<TITLE>Today's interesting Wikipedia pages</TITLE>\n")
	FILE.write("<h2><center>Top Wikipedia Pages</center></h2>")
	FILE.write("<BODY><CENTER>For "+time.strftime("%D")+".</CENTER>\n ")
def TABLEHeader(FILE,RECPERTABSET):
	FILE.write("<center><h3><a href=\"hot.html\">Hot Pages,</a>")
	FILE.write("<a href=\"cold.html\">Cold,</a>")
	FILE.write("<a href=\"new.html\">New Pages</a></h3></center>")
	FILE.write("<div id=\"wrapper\">\n")
	FILE.write("<div id=\"tabContainer\">\n")
def TABLEFooter(FILE):
	FILE.write("</div></div>\n")
def HTMLFooter(FILE):
	FILE.write("<script src=\"tabs.js\"></script>")
	FILE.write("</body></html>\n")

def TABLEBuildTabs(FILE,RECPERTABSET):
	FILE.write("<div class=\"tabs\">\n")
	FILE.write("<ul>")
	for place in range(RECPERTABSET):
		ID="tabheader_"+str(place+1)
		FILE.write("<li id=\""+ID+"\">"+str(place+1)+"</li>\n")
	FILE.write("</ul>\n")
	FILE.write("</div>\n")

def TABLEPopTabs(FILE,QUERY):
	LOGFILE=open("/tmp/tmp.new","a")
	FILE.write("<div class=\"tabscontent\">\n")
	COUNT=0
	TOTALNEW=0
	MLINK="http://en.wikipedia.org/wiki/"
	for item in QUERY:
	        COUNT=0
	        TITLE=''
	        YQUERY=db.tophits.find({'m':MONTH,'y':YEAR,'id':item['id']})
	        for row in YQUERY:
	                COUNT+=1
	        if TOTALNEW>24:
	                break
		if COUNT > 100000:
			break
	        if (COUNT==1 or COUNT==0) and TOTALNEW<25:
	                TOTALNEW+=1
	                NQUERY=db.map.find({'_id':str(item['id'])})
	                for it in NQUERY:
				URLLINK=MLINK+it['title']
	                        TITLE=it['title']
				TITLE=it['title'].encode('utf-8')
				TITLE=string.replace(TITLE,'_',' ')
			ID="tabpage_"+str(TOTALNEW)
			FILE.write("\n<div class=\"tabpage\" id=\""+ID+"\">\n")
	                FILEstr=("<h2>"+str(TOTALNEW)+".</h2><p><a href=\""+URLLINK+"\">"+urllib2.unquote(TITLE)+"</a>\n")
#	                FILEstr=("<h2>"+str(TOTALNEW)+".</h2><p>"+URLLINK+"<br />"+urllib2.unquote(TITLE)+"</a><br />")
			FILE.write(FILEstr)
			FILE.write("<br />Debuting in place: "+str(item['place'])+"\n")
			WHYQUERY=db.why.find({'_id':item['id']})
			FILE.write("<br />Why so popular?<br />\n")
			for entry in WHYQUERY:
				FILE.write('<br />'+entry['text']+'<br />\n')
			LOGFILE.write(str(item['id'])+' '+urllib2.unquote(TITLE)+'\n')
			FILE.write("</p></div>\n")
	FILE.write("</div>\n")
	LOGFILE.close()
	
TODAY=date.today()
DAY=TODAY.day
MONTH=TODAY.month
YEAR=TODAY.year
RECPERTABSET=10
HTMLDIR="/var/www/html/"
HTMLFILE=HTMLDIR+"/debuts_"+str(YEAR)+"_"+str(MONTH)+"_"+str(DAY)+".html"
conn=Connection()
db=conn.wc
QUERY=db.tophits.find({'d':int(DAY)}).sort('place',1) 
COLDQUERY=db.tmpHot.find().sort('delta',1).limit(RECPERTABSET)


OFILE=open(HTMLFILE,"w")
HTMLHeader(OFILE)

TABLEHeader(OFILE,RECPERTABSET)
TABLEBuildTabs(OFILE,RECPERTABSET)
TABLEPopTabs(OFILE,QUERY)
TABLEFooter(OFILE)

HTMLFooter(OFILE)
OFILE.close()
