from functions import wikicount 
import os

DAY,MONTH,YEAR,HOUR,expiretime=wikicount.fnReturnTimes()
HOUR=wikicount.minusHour(int(HOUR))
DAY,MONTH,HOUR=wikicount.fnFormatTimes(DAY,MONTH,HOUR)
wikicount.fnSetStatusTime('p70_export',0)
DAYKEY=str(YEAR)+"_"+str(MONTH)+"_"+str(DAY)
OPTIONS=" -d wc -c hitsdaily -q '{\""+str(DAYKEY)+"\":{\"$exists\":true}}' --fields "+str(DAYKEY)+",\"_id\" --csv --out /home/ec2-user/mongo.csv"
print OPTIONS
os.system("mongoexport "+str(OPTIONS))

os.system("sed -i 1d /home/ec2-user/mongo.csv")
wikicount.fnSetStatusTime('p70_export',1)
