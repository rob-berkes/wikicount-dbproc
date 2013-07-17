#!/usr/bin/python
from functions import wikicount 
import os
import syslog

STARTTIME=wikicount.fnStartTimer()
syslog.syslog('p70_export.py: starting...')
DAY,MONTH,YEAR,HOUR,expiretime=wikicount.fnReturnTimes()
HOUR=wikicount.minusHour(int(HOUR))
DAY,MONTH,HOUR=wikicount.fnFormatTimes(DAY,MONTH,HOUR)
wikicount.fnSetStatusMsg('p70_export',0)
DAYKEY=str(YEAR)+"_"+str(MONTH)+"_"+str(DAY)
OPTIONS=" -d wc -c hitsdaily -q '{\""+str(DAYKEY)+"\":{\"$exists\":true}}' --fields "+str(DAYKEY)+",\"_id\" --csv --out /home/ec2-user/mongo.csv"
print OPTIONS
os.system("mongoexport "+str(OPTIONS))

os.system("sed -i 1d /home/ec2-user/mongo.csv")

for lang in ['ru',]:
	hdCOLL=str(lang)+"_hitsdaily"
	outfile="/home/ec2-user/"+str(lang)+"_mongo.csv"
	OPTIONS=" -d wc -c "+hdCOLL+" -q '{\""+str(DAYKEY)+"\":{\"$exists\":true}}' --fields "+str(DAYKEY)+",\"_id\" --csv --out "+str(outfile)
	os.system("mongoexport "+str(OPTIONS))
	os.system("sed -i 1d "+str(outfile))


RUNTIME=wikicount.fnEndTimerCalcRuntime(STARTTIME)
syslog.syslog('p70_export.py: runtime '+str(RUNTIME)+' seconds.')
wikicount.fnSetStatusMsg('p70_export',3)
wikicount.fnLaunchNextJob('p70_export')
