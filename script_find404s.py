from pymongo import Connection
import httplib
import hashlib
conn=Connection()
db=conn.wc
hconn=httplib.HTTPConnection("en.wikipedia.org")
for line in db.en_threehour.find():
    HASH=hashlib.sha1(line['title']).hexdigest()
    TestURL='/wiki/'+str(line['title'])
    hconn=httplib.HTTPConnection("en.wikipedia.org")
    hconn.request("HEAD",TestURL)
    STATUS=hconn.getresponse().status
    if STATUS==404:
            print line['title'],'\''+HASH+'\''

