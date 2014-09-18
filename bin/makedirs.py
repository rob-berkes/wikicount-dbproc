import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))
from lib import wikilib

LANGS= wikilib.getLanguageList()
PATHS=['action','staging','ondeck']
for lang in LANGS:
	for path in PATHS:
		DIRMAKE="/tmp/"+str(lang)+"_"+str(path)
		if not os.path.exists(DIRMAKE):
			os.makedirs(DIRMAKE)

os.makedirs('/tmp/staging/')
