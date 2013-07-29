import os
LANG='en.s'
FOLDERS=['action','category','image','ondeck','staging']
for folder in FOLDERS:
	NEWPATH="/tmp/"+str(LANG)+"_"+str(folder)
	if not os.path.exists(NEWPATH):
		os.makedirs(NEWPATH)
