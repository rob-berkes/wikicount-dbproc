#/usr/bin/python
import glob 
import re
import os
import shutil 
import string
from functions import wikicount 

DAY,MONTH,YEAR,HOUR,expiretime=wikicount.fnReturnTimes()
DAY,MONTH,HOUR=wikicount.fnFormatTimes(DAY,MONTH,HOUR)

for FILENAME in glob.glob('/tmp/staging/q*'):
	print FILENAME
	IFILE=open(FILENAME,"r")
        OUTFILENAME=string.replace(FILENAME,'staging','ondeck')
	OFILE=open(OUTFILENAME,"w")
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
		ptnFile=re.search("File:",record[1])
		ptnCategory=re.search("Category:",record[1])
		ptnCTalk=re.search("Category_talk:",record[1])
		ptnSTalk=re.search("talk:",record[1])
		ptnImage=re.search("Image:",record[1])
		ptnPhp=re.search(".php",record[1])
		if  not ptnImage and not ptnPhp and not ptnTalk and not ptnUser and not ptnWiki and not ptnSpecial and not ptnUTalk and not ptnTemplate and not ptnWTalk and not ptnFile and not ptnCategory and not ptnCTalk and not ptnSTalk:
			OFILE.write(line)
	os.remove(FILENAME)

