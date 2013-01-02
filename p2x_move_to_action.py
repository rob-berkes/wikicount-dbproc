#/usr/bin/python
import glob
import os 
import string

for FILENAME in glob.glob('/tmp/ondeck/q*.fltr'):
	IFILE=open(FILENAME,'r')
	OFILENAME=string.replace(FILENAME,'ondeck','action')
	OFILE=open(OFILENAME,'w')
	for line in IFILE: 
		OFILE.write(line)
	IFILE.close()
	OFILE.close() 
	os.remove(FILENAME)

