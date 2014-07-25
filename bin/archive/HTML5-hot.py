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
	FILE.write("<center><h3><center>Hottest trending Wikipedia Pages for "+time.strftime("%D")+"</center></h3>")
	FILE.write('<h3>Yesterday\'s '+str(RECPERTABSET)+' hottest Wikipedia articles.</h3></center>')
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
        FILE.write("<div class=\"tabscontent\">\n")
        PLACE=1
        for line in QUERY:
                ID="tabpage_"+str(PLACE)
                FILE.write("<div class=\"tabpage\" id=\""+ID+"\">\n")
                HASH=line['id']
                SEARCHQ={'_id':str(HASH)}
                SEARCH=db.map.find(SEARCHQ)
                for item in SEARCH:
                        TITLE=item['title']
                        TITLE=string.replace(TITLE,'_',' ')
                        TITLE=TITLE.encode('utf-8')
                        NTITLE=urllib2.unquote(TITLE)
                        FILE.write("<h2> "+str(PLACE)+"</h2><p>"+NTITLE+"</p></div>\n")
                PLACE+=1
        OFILE.write("</div>\n")







	
TODAY=date.today()
DAY=TODAY.day
MONTH=TODAY.month
YEAR=TODAY.year
RECPERTABSET=10
HTMLDIR="/var/www/html/"
HTMLFILE=HTMLDIR+"/hot_"+str(YEAR)+"_"+str(MONTH)+"_"+str(DAY)+".html"
conn=Connection()
db=conn.wc
QUERY=db.tmpHot.find().sort('delta',-1).limit(RECPERTABSET)

OFILE=open(HTMLFILE,"w")
HTMLHeader(OFILE)

TABLEHeader(OFILE,RECPERTABSET)
TABLEBuildTabs(OFILE,RECPERTABSET,QUERY)
QUERY.rewind()
TABLEPopTabs(OFILE,QUERY)
TABLEFooter(OFILE)

HTMLFooter(OFILE)
OFILE.close()
