#/usr/bin/python
from pymongo import Connection
from datetime import date
import time 
import string
import urllib2


def HTMLHeader(FILE):
	FILE.write("<HTML><HEAD><link href=\"style.css\" rel=\"stylesheet\" type=\"text/css\"></HEAD>\n")
	FILE.write("<TITLE></TITLE>\n")
	FILE.write("<BODY>\n ")
	FILE.write('<script src="tabs.js"></script>')
def TABLEHeader(FILE,RECPERTABSET):
#	FILE.write("<center><h3><a href=\"hot.html\">Hot Pages,</a>")
#	FILE.write("<a href=\"cold.html\">Cold,</a>")
#	FILE.write("<a href=\"new.html\">New Pages</a></h3></center>")
	FILE.write("<div id=\"wrapper\">\n")
	FILE.write("<div id=\"tabContainer\">\n")
def TABLEFooter(FILE):
	HTMLAdSense(FILE)
	FILE.write("</div></div>\n")
def HTMLFooter(FILE):
	FILE.write('</body></html>\n')
def HTMLAdSense(FILE):
	FILE.write('<center><script type="text/javascript"><!--\n')
	FILE.write('google_ad_client = "ca-pub-0382768768507923";\n')
	FILE.write('/* TopOPage */\n')
	FILE.write('google_ad_slot = "8807406089";\n')
	FILE.write('google_ad_width = 728;\n')
	FILE.write('google_ad_height = 90;\n')
	FILE.write('//-->\n')
	FILE.write('</script>\n')
	FILE.write('<script type=\"text/javascript\" \nsrc=\"http://pagead2.googlesyndication.com/pagead/show_ads.js\">\n')
	FILE.write('</script></center>\n')
def TABLEBuildTabs(FILE,RECPERTABSET,QUERY):
	FILE.write("<center><h3><center>Fading Wikipedia Pages for "+time.strftime("%D")+"</center></h3>")
	FILE.write('<h3>Yesterday\'s '+str(RECPERTABSET)+' cold Wikipedia articles.</h3></center>')
	FILE.write('<h3>Also:\t\t\tHot\t\t\tCold\t\t\tNewest</h3>')
	HTMLAdSense(FILE)
	FILE.write("<div class=\"tabs\">\n")
	FILE.write("<ul>")
	place=1
	for line in QUERY:
		ID='tabheader_'+str(place)
		SEARCHQ={'_id':line['id']}
		SEARCH=db.map.find(SEARCHQ)
		for item in SEARCH:
			FILE.write("<li id=\""+ID+"\"><h5>"+str(place)+" "+str(item['title'][0:9])+"</h5></li>\n")
		place+=1	
#	for place in range(RECPERTABSET):
#		ID="tabheader_"+str(place+1)
#		FILE.write("<li id=\""+ID+"\">"+str(place+1)+"</li>\n")
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
	        YQUERY=db.tophits.find({'d':int(DAY-1),'m':MONTH,'y':YEAR,'id':item['id']})
	        for row in YQUERY:
	                COUNT+=1
	        if TOTALNEW>(RECPERTABSET-1):
	                break
		if COUNT > 100000:
			break
	        if COUNT==0 and TOTALNEW<RECPERTABSET:
	                TOTALNEW+=1
	                NQUERY=db.map.find({'_id':str(item['id'])})
	                for it in NQUERY:
				URLLINK=MLINK+it['title']
	                        TITLE=it['title']
				TITLE=it['title'].encode('utf-8')
				TITLE=string.replace(TITLE,'_',' ')
			ID="tabpage_"+str(TOTALNEW)
			FILE.write("\n<div class=\"tabpage\" id=\""+ID+"\">\n")
	                FILEstr=("<p><h2>"+str(TOTALNEW)+".<a href=\""+URLLINK+"\">"+urllib2.unquote(TITLE)+"</a></h2></p>\n")
			FILE.write(FILEstr)
			FILE.write("<h4>Debuting at: "+str(item['place'])+"</h4><br />\n")
			WHYQUERY=db.why.find({'_id':item['id']})
			for entry in WHYQUERY:
				FILE.write('<p><h2>'+entry['text']+'</h2></p>\n')
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
HTMLFILE=HTMLDIR+"/new_"+str(YEAR)+"_"+str(MONTH)+"_"+str(DAY)+".html"
conn=Connection()
db=conn.wc
QUERY=db.tophits.find({'d':int(DAY)}).sort('place',1)
OFILE=open(HTMLFILE,"w")
HTMLHeader(OFILE)

TABLEHeader(OFILE,RECPERTABSET)
TABLEBuildTabs(OFILE,RECPERTABSET,QUERY)
QUERY.rewind()
TABLEPopTabs(OFILE,QUERY)
TABLEFooter(OFILE)

HTMLFooter(OFILE)
OFILE.close()
