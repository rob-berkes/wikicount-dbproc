import os
LANG=['it.q','pl.q','ru.q','zh.q']
FOLDERS=['action','category','image','ondeck','staging']
for lang in LANG:
	for folder in FOLDERS:
		NEWPATH="/tmp/"+str(lang)+"_"+str(folder)
		if not os.path.exists(NEWPATH):
			os.makedirs(NEWPATH)
