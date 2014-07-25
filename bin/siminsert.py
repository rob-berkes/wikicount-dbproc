from pymongo import Connection
import hashlib
conn=Connection()
db=conn.wc

def hashme(Sw):
	return hashlib.sha1(Sw).hexdigest()

M=hashme("Breaking Bad")
h1=hashme("Oakland_Raiders")
h2=hashme("Green_Bay_Packers")
h3=hashme("National_Football_League")
h4=hashme("Al_Harris")
h5=hashme("Michigan_Wolverines")
h6=hashme("Heisman_Trophy")

SLIST=(h1,h2,h3,h4)


IREC={'_id':str(M), 'list':SLIST}

print IREC
db['en_dsimilars'].insert(IREC)
