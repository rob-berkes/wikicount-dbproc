import heapq
import time 
from multiprocessing import Process, Pipe, Queue
from lib import sorting


IFILE=open('en_918_hitsdailyexport','r')
SORTME=[]
for line in IFILE:
	line=line.strip().split(',')
        HASH=line[0].replace("\"","")
	HITS=int(line[1])
		
        rec=(HITS,HASH)
        SORTME.append(rec)
IFILE.close()
print "Imported file with "+str(len(SORTME))+" records.\n"



#Do the heap sortin
START=time.time()
heapq.heapify(SORTME)
SORTED=[]
MAX_VAL_EL=0
for a in range(0,len(SORTME)):
	ELEMENT=heapq.heappop(SORTME)
	SORTED.append(ELEMENT)
END=time.time()
OFILE=open('918_hd.heap','w')
for line in SORTED:
	OFILE.write(str(line[0])+','+line[1]+'\n')
OFILE.close()
print "Heapsort took "+str(END-START)+" seconds.\n"






IFILE=open('en_918_hitsdailyexport','r')
SORTME=[]
for line in IFILE:
	line=line.strip().split(',')
        HASH=line[0].replace("\"","")
	HITS=int(line[1])
		
        rec=(HITS,HASH)
        SORTME.append(rec)
IFILE.close()
print "Imported file with "+str(len(SORTME))+" records.\n"


#Do some quick sortin
START=time.time()
SORTED=[]
#pconn,cconn=Pipe()
#n=1 #of procs
#p=Process(target=sorting.QuickSortListArray,args=(SORTME,cconn,n))
#p.start()
#SORTED=pconn.recv()
#p.join()
SORTED=sorting.QuickSortListArray(SORTME)
OFILE=open("918_hd.qs","w")
for a in SORTED:
	OFILE.write(str(a[0])+','+a[1]+'\n')
OFILE.close()
END=time.time()
print "Quicksort took "+str(END-START)+" seconds.\n"
