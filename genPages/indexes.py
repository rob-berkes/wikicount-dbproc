#/usr/bin/python
import os
from datetime import date

TODAY=date.today()
DAY=TODAY.day
MONTH=TODAY.month
YEAR=TODAY.year


HTMLDIR="/var/www/html/"
SRCFILE=HTMLDIR+"master_"+str(YEAR)+"_"+str(MONTH)+"_"+str(DAY)+".html"
DSTFILE=HTMLDIR+"master.html"
if os.path.exists(DSTFILE): 
	os.remove(DSTFILE)
os.symlink(SRCFILE,DSTFILE)


SRCFILE=HTMLDIR+"master_"+str(YEAR)+"_"+str(MONTH)+"_"+str(DAY)+".html"
DSTFILE=HTMLDIR+"index.html"
if os.path.exists(DSTFILE):
	os.remove(DSTFILE)
os.symlink(SRCFILE,DSTFILE)

#SRCFILE=HTMLDIR+"hot_"+str(YEAR)+"_"+str(MONTH)+"_"+str(DAY)+".html"
#DSTFILE=HTMLDIR+"hot.html"
#if os.path.exists(DSTFILE):
#	os.remove(DSTFILE)
#os.symlink(SRCFILE,DSTFILE)


#SRCFILE=HTMLDIR+"top_"+str(YEAR)+"_"+str(MONTH)+"_"+str(DAY)+".html"
#DSTFILE=HTMLDIR+"index.html"
#if os.path.exists(DSTFILE):
#	os.remove(DSTFILE)
#os.symlink(SRCFILE,DSTFILE)
