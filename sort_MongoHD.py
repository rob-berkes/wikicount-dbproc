from lib import sorting 
from functions import wikicount
from multiprocessing import Process,Pipe,Queue
import random
import sys
import os
import string 
if __name__ == '__main__' :
	DAY,MONTH,YEAR,HOUR,expiretime=wikicount.fnReturnTimes()
	os.system("sed -i 1d /home/ec2-user/mongo.csv")
	sys.setrecursionlimit(2000)
	n=7  #number partitions to break into
	IFILE=open("/home/ec2-user/mongo.csv","r")
	SORTME=[]
	for line in IFILE:
		line=line.strip('"').split(',')
		HASH=line[1].replace("\"","")
		rec=(line[0],HASH)
		SORTME.append(rec)
	IFILE.close()

	print 'done reading list .... starting mulitple procs'
	pconn,cconn=Pipe()
	lyst=[]
	p=Process(target=sorting.QuickSortMPListArray,args=(SORTME,cconn,n))
	p.start()
	print 'main proc started'

	lyst=pconn.recv()
	print 'starting out write'
	print 'joining child procs'
	p.join()
	OFILE=open("/home/ec2-user/mongo.csv.sorted","w")
	for a in lyst:
		OFILE.write(str(a[0])+','+a[1])
	OFILE.close()
	print 'all done!'
