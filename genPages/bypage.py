#/usr/bin/python
from pymongo import Connection
import hashlib

conn=Connection()
db=conn.wc
TITLETOFIND="Nile_crocodile"
OUTFILE="html/"+str(TITLETOFIND)+".htm"

HASH=hashlib.sha1(TITLETOFIND).hexdigest()

SEARCHQUERY={"id":HASH}

QUERY=db.tophits.find(SEARCHQUERY).sort([('y',1),('m',1),('d',1)]).limit(20)
OFILE=open(OUTFILE,"w")
OFILE.write("<html><head></head>")
OFILE.write("<title>Report for term: "+str(TITLETOFIND)+".</title>\n")
OFILE.write("<table>")
for result in QUERY:
	OFILE.write('<tr><td>'+str(result["m"])+'/'+str(result["d"])+'/'+str(result["y"])+'</td>\n')
	OFILE.write('<td>'+str(result['place'])+'</td></tr>\n')

OFILE.write("</table></html>")

