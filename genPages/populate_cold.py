from pymongo import Connection
from multiprocessing import Process
from functions import wikicount
import urllib2
def cold():
    COLD_LIST_QUERY = db.tmpHot.find().sort('delta', 1).limit(100)
    for p in COLD_LIST_QUERY:
        NAMEQ = db.hitsdaily.findOne({'_id': p['_id']})
        rec = {'title': NAMEQ['title'], 'place': p['orPlace'], 'Hits': p['delta'], 'linktitle': NAMEQ['title'],
               '_id': p['_id']}
        db.prodcold.insert(rec)

        return
STARTTIME = wikicount.fnStartTimer()
wikicount.syslog('populate_cold:  starting...')
DAY, MONTH, YEAR, HOUR, expiretime = wikicount.fnReturnTimes()
conn = Connection()
db = conn.wc
wikicount.fnSetStatusMsg('populate_cold', 0)
db.prodcold.remove()

p = Process(target=cold, args=())
p.start()
p.join()

RUNTIME = wikicount.fnEndTimerCalcRuntime(STARTTIME)
wikicount.syslog('populate_cold: runtime is ' + str(RUNTIME) + ' seconds.')
wikicount.fnSetStatusMsg('populate_cold', 3)
wikicount.fnLaunchNextJob('populate_cold')
