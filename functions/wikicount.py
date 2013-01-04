from pymongo import Connection 
import string
import urllib2

conn=Connection()
db=conn.wc

def MapQuery_FindName(id):
        QUERY={'id':id}
        MAPQ=db.map.find({'_id':id})
        title=''
        utitle=''
        for name in MAPQ:
                        title=name['title']
                        s_title=string.replace(title,'_',' ')
                        t_title=s_title.encode('utf-8')
                        utitle=urllib2.unquote(t_title)


        return title, utitle

