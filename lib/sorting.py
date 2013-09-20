import random
from multiprocessing import Process,Manager,Pipe,Queue
def QuickSort(A):
        if len(A)==1:
                return A
        elif len(A)==0:
                return A
        else:
                PivotIndex=random.randint(0,len(A)-1)
                PivotValue=int(A.pop(PivotIndex))
                lesser=[]
                greater=[]
                pv=[]
                pv.append(PivotValue)
                for val in range(0,len(A)):
                        if int(A[val]) <= PivotValue:
                                lesser.append(int(A[val]))
			elif int(A[val]) == PivotValue:
				pv.append(int(A[val]))
                        else:
                                greater.append(int(A[val]))
        return QuickSort(lesser)+pv+QuickSort(greater)
def QuickSortSiMPle(A,Q,SplitDepth):
	exitQ=Queue()
	totalQueue=Queue()
	SplitDepth-=1
        if len(A)==1:
		Q.put(A)
                return 
        elif len(A)==0:
                return 
        elif SplitDepth<=1:
		sortedlist=QuickSort(A)
		for z in sortedlist:
			Q.put(z)
		return 
	else:
                PivotIndex=random.randint(0,len(A)-1)
                PivotValue=int(A[PivotIndex])
                lesser=[]
                greater=[]
                pv=[]
                pv.append(PivotValue)
		
		lesser=[x for x in A if x < PivotValue]
		greater=[x for x in A if x > PivotValue]
		equals=[x for x in A if x == PivotValue]
		pL=Process(target=QuickSortSiMPle,args=(lesser,exitQ,SplitDepth))
		for y in equals:
			exitQ.put(y)
		pR=Process(target=QuickSortSiMPle,args=(greater,exitQ,SplitDepth))
        return 
def QuickSortListArray(A):
        if len(A)<=1:
                return A
        else:
                PivotIndex=random.randint(0,len(A)-1)
                PivotValue=A.pop(PivotIndex)
                lesser=[x for x in A if int(x[0]) > int(PivotValue[0])]
                greater=[x for x in A if int(x[0]) < int(PivotValue[0])]
                pv=[x for x in A if int(x[0]) == int(PivotValue[0])]
		pv.append(PivotValue)
        return QuickSortListArray(lesser)+pv+QuickSortListArray(greater)

def QuickSortMPListArray(A,conn,NumProcs):
        if len(A)<=1 :
		conn.send(A)
		conn.close()
	elif int(NumProcs)<1:
		conn.send(QuickSortListArray(A))
		conn.close()
        else:
		lesser=[]
		greater=[]
		pv=[]
		Pivot=A.pop(0)
		lesser=[x for x in A if int(x[0]) > int(Pivot[0])]
	        greater=[x for x in A if int(x[0]) < int(Pivot[0])]
		pv=[x for x in A if int(x[0]) == int(Pivot[0])]
		pv.append(Pivot)
		Procs=int(NumProcs)-1
		pConnLeft,cConnLeft=Pipe()
		leftProc=Process(target=QuickSortMPListArray,args=(lesser,cConnLeft,Procs))
		pConnRight,cConnRight=Pipe()
		rightProc=Process(target=QuickSortMPListArray,args=(greater,cConnRight,Procs))
		

		leftProc.start()
		rightProc.start()
		conn.send(pConnLeft.recv()+pv+pConnRight.recv())
		conn.close()
	
		leftProc.join()
		rightProc.join()
        return
#def QuickSortMPWikiList(A,conn,NumProcs,SORTVAR):
#	print str(len(A))+' starting mplarray'
#       if len(A)<=1 :
#		print 'single num reached'
#		conn.send(A)
#		conn.close()
#	elif int(NumProcs)<1:
#		print 'proc limit reached, smp qs'
#		conn.send(QuickSortListArray(A))
#		conn.close()
#       else:
#		lesser=[]
#		greater=[]
#		pv=A.pop(0)
#		print 'Partition Value: '+str(pv[0])
#		lesser=[x for x in A if int(x[0]) > int(pv[0])]
#	        greater=[x for x in A if int(x[0]) < int(pv[0])]
#		pv=[x for x in A if x[0] == pv[0]]
#		Procs=int(NumProcs)-1
#		pConnLeft,cConnLeft=Pipe()
#		leftProc=Process(target=QuickSortMPListArray,args=(lesser,cConnLeft,Procs))
#		pConnRight,cConnRight=Pipe()
#		rightProc=Process(target=QuickSortMPListArray,args=(greater,cConnRight,Procs))
#		
#
#		leftProc.start()
#		rightProc.start()
#		print 'mplarray send'
#		conn.send(pConnLeft.recv()+pv+pConnRight.recv())
#		conn.close()
#	
#		leftProc.join()
#		rightProc.join()
#	return
#def QuickSortStub(A,conn,NumProcs):
#
#	return
#def QuickSort(A,IndexValue):
#        if len(A)==1:
#                return A
#        elif len(A)==0:
#                return A
#        else:
#                PivotIndex=random.randint(0,len(A)-1)
#                PivotValue=A.pop(PivotIndex)
#                lesser=()
#                greater=()
#                for val in range(0,len(A)):
#                        if A[val][IndexValue] <= PivotValue[IndexValue]:
#                                lesser.append(A[val])
#                        else:
#                                greater.append(A[val])
#                pv=[]
#                pv.append(PivotValue)
#        return QuickSort(lesser)+pv+QuickSort(greater)
def BubbleSort(A):
        swap_done=True
        while swap_done:
                swap_done=False
                for valu in range(0,len(A)-1):
                        if A[valu] > A[valu+1]:
                                swap_done=True
                                t=A[valu+1]
                                A[valu+1]=A[valu]
                                A[valu]=t

        return A

