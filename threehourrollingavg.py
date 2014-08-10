#coding: utf-8
'''
 This module uses the difference between three hour rolling averages taken and stored each hour in lang_lastrollavg
'''
from operator import itemgetter
import syslog
from numpy import mean, array
from pymongo import Connection
from lib import wikicount

STARTTIME = wikicount.fnStartTimer()
conn = Connection()
db = conn.wc
DAY, MONTH, YEAR, HOUR, expiretimes = wikicount.fnReturnTimes()
HOUR = wikicount.minusHour(int(HOUR))
HOUR, HOUR2, HOUR3 = wikicount.fnReturnLastThreeHours(HOUR)
HOUR = wikicount.fnStrFmtDate(HOUR)
HOUR2 = wikicount.fnStrFmtDate(HOUR2)
HOUR3 = wikicount.fnStrFmtDate(HOUR3)
wikicount.fnSetStatusMsg('threehrrollingavg', 0)
hourlies = []
TypeErrors = 0
KeyErrors = 0
z = 1
LANGUAGES = wikicount.getLanguageList()
for lang in LANGUAGES:
    hourlies = []
    KeyErrors = 0
    TypeErrors = 0
    hhdTABLE = str(lang)+"_hitshourlydaily"
    hdTABLE = str(lang)+"_hitsdaily"
    outTABLE = str(lang)+"_threehour"
    lastTABLE = str(lang)+"_lastrollavg"
    RESULTS = db[hhdTABLE].find({str(HOUR):{'$exists':True}}).sort(str(HOUR), -1).limit(2000)
    syslog.syslog(str(lang)+" : "+str(RESULTS.count()))
    for item in RESULTS:
        try:
            QUERYtitle = db[hdTABLE].find_one({'_id':item['_id']})
            LASTQUERY = db[lastTABLE].find_one({'_id':item['_id'],'hour':HOUR})
            atitle = QUERYtitle['title']
            title, utitle = wikicount.FormatName(atitle)
            try:
                b1 = item[HOUR]
            except KeyError:
                b1 = 0
            try:
                b2 = item[HOUR2]
            except KeyError:
                b2 = 0
            try:
                b3 = item[HOUR3]
            except KeyError:
                b3 = 0
 
            rollingavg = mean(array([b1, b2, b3]))
            try:
		LASTAVG = LASTQUERY['rollavg']
	    except:
                LASTAVG = 0
            lastrollavg = rollingavg-LASTAVG		
            rec = {'title':atitle, 'rollavg':int(lastrollavg), 'id':item['_id']}
            hourlies.append(rec)
            nrec = {'title':atitle, 'rollavg':int(lastrollavg), 'id':item['_id'], 'hour':HOUR}
            db[lastTABLE].update(nrec,upsert=True)
        except TypeError:
	    print 'uhoh!'
    z = 1
    db[outTABLE].remove()
    for w in sorted(hourlies, key=itemgetter('rollavg'), reverse=True):
        if z < 101:
            rec = {'place':z, 'title':w['title'],'rollavg':w['rollavg'], 'id':w['id']}
            db[outTABLE].insert(rec)
            z += 1
