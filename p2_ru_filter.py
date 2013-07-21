#/usr/bin/python
#coding: utf-8
import glob 
import re
import os
import shutil 
import string
from functions import wikicount
from time import time
import syslog
 
DAY,MONTH,YEAR,HOUR,expiretime=wikicount.fnReturnTimes()
DAY,MONTH,HOUR=wikicount.fnFormatTimes(DAY,MONTH,HOUR)
wikicount.fnSetStatusMsg('p2_filter',0)
syslog.syslog('p2_filter.py: starting....')
LANGLIST=wikicount.getLanguageList()
for lang in LANGLIST:
	a=time()
	for FILENAME in glob.glob('/tmp/'+str(lang)+'_staging/q*'):
		print FILENAME
		IFILE=open(FILENAME,"r")
        	OUTFILENAME=string.replace(FILENAME,'staging','ondeck')
		IMGFILENAME=string.replace(FILENAME,'staging','image')
		CATFILENAME=string.replace(FILENAME,'staging','category')
		OFILE=open(OUTFILENAME,"w")
		IMGFILE=open(IMGFILENAME,"a")
		CATFILE=open(CATFILENAME,"a")
		for line in IFILE:
			record=line.strip().split()
			TITLE=record[1].decode('unicode_escape')
			TITLE=TITLE.strip('(){}')
			ptnTalk=re.search("Talk:",record[1])
			ptnUser=re.search("User:",record[1])
			ptnWiki=re.search("Wikipedia:",record[1])
			ptnSpecial=re.search("Special:",record[1])
			ptnUTalk=re.search("User_talk:",record[1])
			ptnTemplate=re.search("Template:",record[1])
			ptnWTalk=re.search("Wikpedia_talk:",record[1])
			ptnFile=re.search("Файл",record[1])
			ptnCategory=re.search("Категория",record[1])
			ptnCTalk=re.search("Category_talk:",record[1])
			ptnSTalk=re.search("talk:",record[1])
			ptnImage=re.search("Image:",record[1])
			ptnPhp=re.search(".php",record[1])
				
			if ptnCategory:
				CATFILE.write(line)
			if ptnImage or ptnFile:
				IMGFILE.write(line)
			if  not ptnImage and not ptnPhp and not ptnTalk and not ptnUser and not ptnWiki and not ptnSpecial and not ptnUTalk and not ptnTemplate and not ptnWTalk and not ptnFile and not ptnCategory and not ptnCTalk and not ptnSTalk:
				OFILE.write(line)
		OFILE.close()
		IMGFILE.close()
		CATFILE.close()
		os.remove(FILENAME)

	b=time()
	c=b-a
	d=round(c,3)
	syslog.syslog("p2_ru_fi: Language: "+str(lang)+" runtime is "+str(d)+" seconds.")
#wikicount.fnSetStatusMsg('p2_ru_filter',3)
wikicount.fnLaunchNextJob('p2_ru_filter')
