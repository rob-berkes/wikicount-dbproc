#/usr/bin/python
from datetime import date
import time
import string
import urllib2

from pymongo import Connection


def HTMLHeader(FILE):
	FILE.write("<!DOCTYPE html>")
	FILE.write("<HTML><HEAD><link href=\"style.css\" rel=\"stylesheet\" type=\"text/css\">\n")
	FILE.write("<TITLE>Wikipedia Page Trends</TITLE></HEAD>\n")
	FILE.write("<BODY>\n ")
	GoogleAnalytics(FILE)
	FILE.write('\n<script src="tabs.js"></script>')
def TABLEHeader(FILE,RECPERTABSET):
	FILE.write("<div id=\"wrapper\">\n")
	FILE.write("<div id=\"tabContainer\">\n")
def TABLEFooter(FILE):
	FILE.write("</div>\n</div>\n</div>")
#	AmazonMyFaves(FILE)
def HTMLFooter(FILE):
	FILE.write('<img src="http://www.darryl.com/vipower.gif" />')
	FILE.write('</body></html>\n')
def AmazonMyFaves(FILE):
	FILE.write('<div class="AdCenter">')
	FILE.write('<SCRIPT charset="utf-8" type="text/javascript" src="http://ws.amazon.com/widgets/q?rt=tf_mfw&ServiceVersion=20070822&MarketPlace=US&ID=V20070822/US/wikitrendsinf-20/8001/2936d892-e92b-4f58-9ea4-5d82b3526fec"> </SCRIPT> <NOSCRIPT><A HREF="http://ws.amazon.com/widgets/q?rt=tf_mfw&ServiceVersion=20070822&MarketPlace=US&ID=V20070822%2FUS%2Fwikitrendsinf-20%2F8001%2F2936d892-e92b-4f58-9ea4-5d82b3526fec&Operation=NoScript">Amazon.com Widgets</A></NOSCRIPT>\n')
	FILE.write('</div>')
def AmazonOmakaseHeader(FILE):
	FILE.write('<div class="AdCenter"')
	FILE.write('<script type="text/javascript"><!--\n')
	FILE.write('amazon_ad_tag = "wikitrendsinf-20"; amazon_ad_width = "728"; amazon_ad_height = "90";//--></script>')
def AmazonOmakaseFooter(FILE):
	FILE.write('<script type="text/javascript" src="http://www.assoc-amazon.com/s/ads.js"></script>')
	FILE.write('</div>')
def GoogleAnalytics(FILE):
	FILE.write('<script type="text/javascript">\n')
	FILE.write('var _gaq = _gaq || [];\n')
	FILE.write('_gaq.push([\'_setAccount\', \'UA-36823844-1\']);\n')
	FILE.write('_gaq.push([\'_setDomainName\', \'wikitrends.info\']);\n')
	FILE.write('_gaq.push([\'_trackPageview\']);\n')
	FILE.write('(function() {\n')
	FILE.write('var ga = document.createElement(\'script\'); ga.type = \'text/javascript\'; ga.async = true;\n')
	FILE.write('ga.src = (\'https:\' == document.location.protocol ? \'https://\' : \'http://\') + \'stats.g.doubleclick.net/dc.js\';\n')
	FILE.write('var s = document.getElementsByTagName(\'script\')[0]; s.parentNode.insertBefore(ga, s);\n')
	FILE.write('  })();\n')
	FILE.write('</script>\n')
def HTMLAdSense(FILE):
	FILE.write('<script type="text/javascript">\n')
	FILE.write('<!--\n')
	FILE.write('google_ad_client = "ca-pub-0382768768507923";\n')
	FILE.write('/* TopOPage */\n')
	FILE.write('google_ad_slot = "8807406089";\n')
	FILE.write('google_ad_width = 728;\n')
	FILE.write('google_ad_height = 90;\n')
	FILE.write('-->\n')
	FILE.write('</script>\n')
	FILE.write('<div class="AdCenter">')
	FILE.write('<script type=\"text/javascript\" src=\"http://pagead2.googlesyndication.com/pagead/show_ads.js\">\n')
	FILE.write('</script>\n')
	FILE.write('</div>')
def HTMLAdSenseHeader(FILE):
	FILE.write('<div class="BannerAdCenter">')
	FILE.write('<script type="text/javascript"><!--\n')
	FILE.write('google_ad_client = "ca-pub-0382768768507923";\n')
	FILE.write('/* Number1 */\n')
	FILE.write('google_ad_slot = "1416956487";\n')
	FILE.write('google_ad_width = 468;\n')
	FILE.write('google_ad_height = 60;\n')
	FILE.write('//-->\n')
	FILE.write('</script>\n')

def HTMLAdSenseFooter(FILE):
	FILE.write('<div class="AdCenter">\n')
	FILE.write('<script type=\"text/javascript\" src=\"http://pagead2.googlesyndication.com/pagead/show_ads.js\">\n')
        FILE.write('</script>\n')
	FILE.write('</div>\n</div>\n')

def HTMLAdSenseNumber1(FILE):
	FILE.write('<script type="text/javascript"><!--\n')
	FILE.write('google_ad_client = "ca-pub-0382768768507923";\n')
	FILE.write('/* Number1 */\n')
	FILE.write('google_ad_slot = "1416956487";\n')
	FILE.write('google_ad_width = 468;\n')
	FILE.write('google_ad_height = 60;\n')
	FILE.write('//-->\n')
	FILE.write('</script>\n')
	FILE.write('<script type="text/javascript"\n')
	FILE.write('src="http://pagead2.googlesyndication.com/pagead/show_ads.js">\n')
	FILE.write('</script>\n')
def TABLEBuildTabs(FILE):
	FILE.write("<h2>Wikitrends.info</h2><br />")
	FILE.write("<h3>Trending Wikipedia Pages for "+time.strftime("%D")+"</h3>")
	FILE.write("<div class=\"tabs\">\n")
	FILE.write("<ul>")
	FILE.write("<li id=\"tabheader_1\"><h5>Top</h5></li>\n")
	FILE.write("<li id=\"tabheader_2\"><h5>Newest</h5></li>\n")
#	FILE.write("<li id=\"tab_3\"><h5>Hot</h5></li>\n")
#	FILE.write("<li id=\"tab_4\"><h5>Cold</h5></li>\n")
	FILE.write("</ul>\n")
	FILE.write("</div>\n")
	ID="tabpage_1"
	FILE.write("<div class=\"CategoryText\" id=\""+ID+"\">\"")
	FILE.write("These are yesterday's most popular Wikipedia articles, recalculated every 2 hours. Thanks to the Wikimedia.org website and Domas Mituzas and his stat files.")
	FILE.write("</div>")
	ID="tabpage_2"
	FILE.write("<div class=\"CategoryText\" id=\""+ID+"\">\"")
	FILE.write("This page lists the most popular articles to debut among Wikipedia's top quarter million.")
	FILE.write("</div>")
	
def findTopicAdText(HASH,FILE):
	QUERY=db.topictext.find({'_id':HASH})
	try:
		for line in QUERY:
			FILE.write("<div class=\"TopicText\">\n")
			FILE.write(line['text']+'<br />\n')
			FILE.write(line['ad'])
			FILE.write("\n</div>\n")
	except KeyError:
		pass

def TABLEPopTabs(FILE,RECPERTABSET):
	LOGFILE=open("/tmp/log.log","a")
	PLACE=1
	FILE.write("<div class=\"tabscontent\">\n")
	ID="tabpage_1"
	FILE.write("<div class=\"tabpage\" id=\""+ID+"\">\n")
	FILE.write("<br /><br />")
	QUERY=db.tophits.find({'d':int(DAY)}).sort('place',1).limit(RECPERTABSET)
	for line in QUERY:
		HASH=line['id']
		SEARCHQ={'_id':str(HASH)}
		SEARCH=db.map.find(SEARCHQ)
		for item in SEARCH:
			TITLE=item['title']
			STITLE=string.replace(TITLE,'_',' ')
			TTITLE=STITLE.encode('utf-8')
			NTITLE=urllib2.unquote(TTITLE)
			if PLACE==15 or PLACE==45:
				HTMLAdSense(FILE)
			if PLACE==30:
				AmazonMyFaves(FILE)
			FILE.write("<a href=\"http://en.wikipedia.org/wiki/"+TITLE.encode('utf-8')+"\">"+str(PLACE)+" "+NTITLE+"</a>\n")
			findTopicAdText(HASH,FILE)

		PLACE+=1
	LOGFILE.close()
	FILE.write("</div>\n")
	FILE.write("<div class=\"tabscontent\">\n")
	ID="tabpage_2"
	QUERY=db.tmpHot.find().sort('delta',1).limit(RECPERTABSET)
        FILE.write("<div class=\"tabpage\" id=\""+ID+"\">\n")
	PLACE=1
	for line in QUERY:
                HASH=line['id']
                SEARCHQ={'_id':str(HASH)}
                SEARCH=db.map.find(SEARCHQ)
                for item in SEARCH:
                        TITLE=item['title']
                        STITLE=string.replace(TITLE,'_',' ')
			STITLE=string.replace(STITLE,'_','%20')
                        TTITLE=STITLE.encode('utf-8')
                        NTITLE=urllib2.unquote(TTITLE)
			if PLACE==15 or PLACE==45:
				HTMLAdSense(FILE)
			if PLACE==30:
				AmazonMyFaves(FILE)
                        FILE.write("<a href=\"http://en.wikipedia.org/wiki/"+TITLE.encode('utf-8')+"\">"+str(PLACE)+" "+NTITLE+"\n")
			if PLACE==10:
				FILE.write('</a>')
			else:
				FILE.write('</a>')
			findTopicAdText(HASH,FILE)
                PLACE+=1

        LOGFILE.close()
	FILE.write("</div>\n")
	FILE.write("</div>\n")
		
TODAY=date.today()
DAY=TODAY.day
MONTH=TODAY.month
YEAR=TODAY.year
RECPERTABSET=50
HTMLDIR="/var/www/html/"
HTMLFILE=HTMLDIR+"/master_"+str(YEAR)+"_"+str(MONTH)+"_"+str(DAY)+".html"
conn=Connection()
db=conn.wc
QUERY=db.tophits.find({'d':int(DAY)}).sort('place',1).limit(RECPERTABSET)
COLDQUERY=db.tmpHot.find().sort('delta',1).limit(RECPERTABSET)


OFILE=open(HTMLFILE,"w")
HTMLHeader(OFILE)

TABLEHeader(OFILE,RECPERTABSET)
TABLEBuildTabs(OFILE)
QUERY.rewind()
TABLEPopTabs(OFILE,RECPERTABSET)
TABLEFooter(OFILE)

HTMLFooter(OFILE)
OFILE.close()
