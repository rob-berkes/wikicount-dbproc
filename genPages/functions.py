from pymongo import Connection
import string
import urllib2
import databaseConnection


def encodeURL(id):
	NQUERY=db.map.find({'_id':str(item['id'])})
        for it in NQUERY:
                   title=it['title']
                   s_title=string.replace(title,'_',' ')
                   s_title=string.replace(s_title,'/','')
                   t_title=s_title.encode('utf-8')
                   utitle=urllib2.unquote(t_title)
	return utitle
	
