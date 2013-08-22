from pymongo import Connection
import httplib
import hashlib
from functions import wikicount
LANGUAGES=wikicount.getLanguageList()
conn=Connection()
db=conn.wc
for lang in LANGUAGES:
	print "report for : "+str(lang)+" language 404s."
	hconn=httplib.HTTPConnection(str(lang)+".wikipedia.org")
	CNAME=str(lang)+"_threehour"
	for line in db[CNAME].find():
	    HASH=hashlib.sha1(line['title']).hexdigest()
	    TestURL='/wiki/'+str(line['title'])
	    hconn=httplib.HTTPConnection(str(lang)+".wikipedia.org")
	    hconn.request("HEAD",TestURL)
	    STATUS=hconn.getresponse().status
	    if STATUS==404:
	#            print line['title'],'\''+HASH+'\''
		    print '\''+HASH+'\''+', 	#'+str(line['title'])

