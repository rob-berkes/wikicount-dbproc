import os
from functions import wikilib
LANGS=wikilib.getLanguageList()
PATHS=['action','staging','ondeck']
for lang in LANGS:
	for path in PATHS:
		DIRMAKE="/tmp/"+str(lang)+"_"+str(path)
		if not os.path.exists(DIRMAKE):
			os.makedirs(DIRMAKE)


