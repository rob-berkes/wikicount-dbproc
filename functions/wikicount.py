#coding: utf-8
from pymongo import Connection 
import string
import urllib2
from datetime import date
import datetime
import time
from wsgiref.handlers import format_date_time
import syslog
import os
import HTMLParser

_htmlparser=HTMLParser.HTMLParser()
unescape=_htmlparser.unescape 

conn=Connection()
db=conn.wc


class logRecord():
    def __init__(self,language,page,views,size):
        self.language=language
        self.page=page
        self.views=views
        self.bwidth=size



def MapQuery_FindName(id):
        QUERY={'id':id}
        MAPQ=db.hitsdaily.find({'_id':id})
        title=''
        utitle=''
        for name in MAPQ:
                        title=name['title']
                        s_title=string.replace(title,'_',' ')
                        t_title=s_title.encode('utf-8')
                        utitle=urllib2.unquote(t_title)
			utitle=unescape(utitle)
        return title, utitle
def MapQuery_FindCategory(id):
        QUERY={'id':id}
        MAPQ=db.category.find_one({'_id':id})
        title=''
        utitle=''
        title=MAPQ['title']
        s_title=string.replace(title,'_',' ')
        t_title=s_title.encode('utf-8')
        utitle=urllib2.unquote(t_title)
        return title, utitle
def MapQuery_FindImageName(id):
        QUERY={'id':id}
        MAPQ=db.image.find_one({'_id':id})
        title=''
        utitle=''
        title=MAPQ['title']
        s_title=string.replace(title,'_',' ')
        t_title=s_title.encode('utf-8')
        utitle=urllib2.unquote(t_title)
        return title, utitle
def fnIsPrevJobDone(CURJOBNAME):
        DAY,MONTH,YEAR,HOUR,expiretime=fnReturnTimes()
        HOUR=minusHour(int(HOUR))
        HOUR=adjustHour(int(HOUR))
	return True
def fnStub1_fnIsPrevJobDone(CURJOBNAME):
	#may be unneeded now that new job scheduling in place, leave for now
       if CURJOBNAME=='p0_dl':
               STATUSQUERY=db.logSystem.find_one({'table':'p0_dl'})
               CSTATUSQ=db.logSystem.find_one({'table':'p1_split'})
               if STATUSQUERY['mesg']==int(HOUR) and CSTATUSQ['mesg']==int(HOUR):
                       syslog.syslog('Job p1_split already run, cron job NOT starting')
               elif STATUSQUERY['mesg']==int(HOUR):
                       return True
       elif CURJOBNAME=='p1_split':
               STATUSQUERY=db.logSystem.find_one({'table':'p1_split'})
               CSTATUSQ=db.logSystem.find_one({'table':'p2_filter'})
               if STATUSQUERY['mesg']==int(HOUR) and CSTATUSQ['mesg']==int(HOUR):
                       syslog.syslog('Job p2_filter already run, cron job NOT starting')
               elif STATUSQUERY['mesg']==int(HOUR):
                       return True
       elif CURJOBNAME=='p2_filter':
               STATUSQUERY=db.logSystem.find_one({'table':'p2_filter'})
               CSTATUSQ=db.logSystem.find_one({'table':'p2x_move_to_action'})
               if STATUSQUERY['mesg']==int(HOUR) and CSTATUSQ['mesg']==int(HOUR):
                       syslog.syslog('Job p2x_move already run, cron job NOT starting')
               elif STATUSQUERY['mesg']==int(HOUR):
                       return True
       elif CURJOBNAME=='p2x_move_to_action':
               STATUSQUERY=db.logSystem.find_one({'table':'p2x_move_to_action'})
               CSTATUSQ=db.logSystem.find_one({'table':'p3_add_to_db'})
               if STATUSQUERY['mesg']==int(HOUR) and CSTATUSQ['mesg']==int(HOUR):
                       syslog.syslog('Job p3_add already run, cron job NOT starting')
               elif STATUSQUERY['mesg']==int(HOUR):
                       return True
       elif CURJOBNAME=='p3_add_to_db':
               STATUSQUERY=db.logSystem.find_one({'table':'p3_add_to_db'})
               CSTATUSQ=db.logSystem.find_one({'table':'p3_image_add'})
               if STATUSQUERY['mesg']==int(HOUR) and CSTATUSQ['mesg']==int(HOUR):
                       syslog.syslog('Job p3_image already run, cron job NOT starting')
               elif STATUSQUERY['mesg']==int(HOUR):
                       return True
       elif CURJOBNAME=='p3_image_add':
               STATUSQUERY=db.logSystem.find_one({'table':'p3_image_add'})
               CSTATUSQ=db.logSystem.find_one({'table':'p3_category_add'})
               if STATUSQUERY['mesg']==int(HOUR) and CSTATUSQ['mesg']==int(HOUR):
                       syslog.syslog('Job p3_category already run, cron job NOT starting')
               elif STATUSQUERY['mesg']==int(HOUR):
                       return True
       elif CURJOBNAME=='p3_category_add':
               STATUSQUERY=db.logSystem.find_one({'table':'p3_category_add'})
               CSTATUSQ=db.logSystem.find_one({'table':'p70_export'})
               if STATUSQUERY['mesg']==int(HOUR) and CSTATUSQ['mesg']==int(HOUR):
                       syslog.syslog('Job p70_export already run, cron job NOT starting')
               elif STATUSQUERY['mesg']==int(HOUR):
                       return True
       elif CURJOBNAME=='p70_export':
               STATUSQUERY=db.logSystem.find_one({'table':'p70_export'})
               CSTATUSQ=db.logSystem.find_one({'table':'p90_remove_noise'})
               if STATUSQUERY['mesg']==int(HOUR) and CSTATUSQ['mesg']==int(HOUR):
                       syslog.syslog('Job p90_remove_noise already run, cron job NOT starting')
               elif STATUSQUERY['mesg']==int(HOUR):
                       return True
       elif CURJOBNAME=='p90_remove_noise':
               STATUSQUERY=db.logSystem.find_one({'table':'p90_remove_noise'})
               CSTATUSQ=db.logSystem.find_one({'table':'sortMongoHD'})
               if STATUSQUERY['mesg']==int(HOUR) and CSTATUSQ['mesg']==int(HOUR):
                       syslog.syslog('Job sortMongoHD already run, cron job NOT starting')
               elif STATUSQUERY['mesg']==int(HOUR):
                       return True
       elif CURJOBNAME=='sortMongoHD':
               STATUSQUERY=db.logSystem.find_one({'table':'sortMongoHD'})
               CSTATUSQ=db.logSystem.find_one({'table':'tophits'})
               if STATUSQUERY['mesg']==int(HOUR) and CSTATUSQ['mesg']==int(HOUR):
                       syslog.syslog('Job tophits already run, cron job NOT starting')
               elif STATUSQUERY['mesg']==int(HOUR):
                       return True
       elif CURJOBNAME=='tophits':
       #       STATUSQUERY=db.logSystem.find_one({'table':'tophits'})
       #       CSTATUSQ=db.logSystem.find_one({'table':'threehrrollingavg'})
       #       if STATUSQUERY['mesg']==int(HOUR) and CSTATUSQ['mesg']==int(HOUR):
       #               syslog.syslog('Job threehrrollingavg already run, cron job NOT starting')
       #       elif STATUSQUERY['mesg']==int(HOUR):
               return True
       elif CURJOBNAME=='threehrrollingavg':
               STATUSQUERY=db.logSystem.find_one({'table':'threehrrollingavg'})
               CSTATUSQ=db.logSystem.find_one({'table':'fillTmpHot'})
               if STATUSQUERY['mesg']==int(HOUR) and CSTATUSQ['mesg']==int(HOUR):
                       syslog.syslog('Job fillTmpHot already run, cron job NOT starting')
               elif STATUSQUERY['mesg']==int(HOUR):
                       return True
       elif CURJOBNAME=='fillTmpHot':
               STATUSQUERY=db.logSystem.find_one({'table':'fillTmpHot'})
               CSTATUSQ=db.logSystem.find_one({'table':'populate_trending'})
               if STATUSQUERY['mesg']==int(HOUR) and CSTATUSQ['mesg']==int(HOUR):
                       syslog.syslog('Job populate_trending already run, cron job NOT starting')
               elif STATUSQUERY['mesg']==int(HOUR):
                       return True
       elif CURJOBNAME=='populate_trending':
               STATUSQUERY=db.logSystem.find_one({'table':'populate_trending'})
               CSTATUSQ=db.logSystem.find_one({'table':'populate_debuts'})
               if STATUSQUERY['mesg']==int(HOUR) and CSTATUSQ['mesg']==int(HOUR):
                       syslog.syslog('Job populate_debuts already run, cron job NOT starting')
               elif STATUSQUERY['mesg']==int(HOUR):
                       return True
       elif CURJOBNAME=='populate_debuts':
               STATUSQUERY=db.logSystem.find_one({'table':'populate_debuts'})
               CSTATUSQ=db.logSystem.find_one({'table':'populate_cold'})
               if STATUSQUERY['mesg']==int(HOUR) and CSTATUSQ['mesg']==int(HOUR):
                       syslog.syslog('Job pop_cold already run, cron job NOT starting')
               elif STATUSQUERY['mesg']==int(HOUR):
                       return True
       elif CURJOBNAME=='populate_cold':
               STATUSQUERY=db.logSystem.find_one({'table':'populate_cold'})
               CSTATUSQ=db.logSystem.find_one({'table':'populate_category'})
               if STATUSQUERY['mesg']==int(HOUR) and CSTATUSQ['mesg']==int(HOUR):
                       syslog.syslog('Job pop_cat already run, cron job NOT starting')
               elif STATUSQUERY['mesg']==int(HOUR):
                       return True
       elif CURJOBNAME=='populate_category':
               STATUSQUERY=db.logSystem.find_one({'table':'populate_category'})
               CSTATUSQ=db.logSystem.find_one({'table':'populate_image'})
               if STATUSQUERY['mesg']==int(HOUR) and CSTATUSQ['mesg']==int(HOUR):
                       syslog.syslog('Job pop_image already run, cron job NOT starting')
               elif STATUSQUERY['mesg']==int(HOUR):
                       return True
       elif CURJOBNAME=='populate_image':
               return True
       return False
def getLanguageList():
	LList=['en','ru','ja','zh','es','fr','pl','pt','it','de','ro','eo','hr','ar','la','sw','af','simple','en.b','en.q','en.s','en.d','en.voy','fr.d','fr.b','sv','ja.b','it.b','de.b','commons.m','it.q','pl.q','ru.q','zh.q']
	return LList
def getBadList(lang):
	enBList=['Grażyna_Plebanek,','Amanda_O`neill,','Philip_C.','Ks._Marian',]
	return enBList
def fnGetSpamList(lang):
	if lang=='en':
		SpamList=['6ce8dbdabc9b9936478d9196007e2ee3864ac1db',
			     'ff255e4bcb8594a7ed26c92d292dc3b442aefb4c',
			     'e9ed3661a6aeab9cb79287e45b4149b6051fb3ef',
			     '64800b72317695df142009c4fe8c06bde2aae00d',
			     'b5bc606f0ffa21182c07abcf4758ad94e45bfb34', #Anita Ganeri, with comma
			     'aa737042d3988ef5277d7867e01fe228c7c4077f', #W_Walce 
			     '1fb36d041828a6bdb5f1fdcd72e9173abb563334', #Phillip_C
			     '3bf7345236012805f564fa244fd433dace4d39bf', #Tadeusz Naserouski
			     'a6166c582b6827501160af830b833a106e03a211', #Bozena Aksamit
			     '3d76578c4f8cb0be75411bb902ddef7d150cc79f', #Maureen Johnson,
			     '079c8c7539635605810064cd4f3ca0bf0d266ddd', #Arkadiusz Marciniak
			     '47760316c811df09dd1ba6b64b59dddc108cd5e3', #David Levien
			     '0dbd8334375fc0f09f85247ec3318aa380640473', #Aleksander Fredro,
			     '5118e9c2ca447be4af9cc12e987175b248877df2', #J._R.
			     '32bdf0f13a86a03dd89fcd42fa90791cfa23e2ac', #Zarys Psychologii
		             'ed67fe416c2802cfceabbe48717ba3b6af2da255', #L._E
		 	     'b9820135e2ce385f5eb84374854c77bba1aeeef4', #L._L_
			     '4a3891325b68f8a20f4f1624822c24c890591402', #Al Switzler
			     'd63c638062355132cd0cb9536e6a0ff9c4d05a0a', #Tomas Jaztrun
			     'c0c3a3e4858a825c6be4a470f2529dcdc63755d9', #Melanie Maddison
			     '9efd6788044203629b8fb9fdb3666cb6a9dbf7c4',
			     '6af42fbe86717da2a5b2254bc060acb8a269571a',
			     '7f1e4cdb5985106b3efdb443c06a590a92c637f0',
			     '61a9a52d62131297c3405f85e8d7f183a42ab496',
			     '14f58e40f51f35f11ff68aeb59a78d2a5c53e370',
			     '4a616bee21e11cb01e92efb2bb53e9f207c3792f',
			     '79ba648642d5eebafb7912925a41a0b89d1793ce',
			     'bf80678c8b7a33134555c34d5d06d756c172dc8b',
			     'de8567bd1e0c5f43bd7010dac36cd16b202c7f94',
			     '955626aef7c2cffaea68f7a16bfc86a21a1cd91f',
			     '1284700d4bdfd784433abae7c856c55069cee13c',
			     '2bcfc9ac2a6d2e0f494d2fe9ebb9cb5e715719c3',
			     'c65536c01867cebb050b2b7208bcf7bc29353e6b',
			     '7261c8b1dcf430050ccd1ad1da7a4280b2413dd4',
			     'f3594556fee36c2e789ce37de9c846304766fa7b',
			     'd90e0187e7958cc2e31d8a882a52dee459e1f330',
			     '12db9e4eeb73167e2abc61dd1261d49e3f171ff5',
			     '4460ade4dc2601bf1e87a40e8a20d69f78836819',
			     '1acece7723467da8404cc1645233daa11f62b8b8',
			     'aa1c52a2654a83c58b958282cf584f07cffb78d9',
			     '61534bb3ce024b6f609ce281b70a648111df7bd5',
			     'dd309538e9e911e8a4d717d1c5073553a09a582e',
			     'a0e12e8f30cfe82316cb3b56cf37bf3ed3048d44',
			     'f6157f0449151ccb95b86abdf51aae3c3deecd95', #?cmd=
			     '9a17f9280ad599ed06310d348ddf729a03830175',
			     '68302be3f3807ebc890215ef29bf0d3c1c28e2ac',
			     'bf53b330c0875317d3e8db35cf1c82d900ec97d3',
			     'f6ece7ccd700925e6e80b7436f4b6a655855bb57',
			     '358f7ca1ed80bc6461f151c3baaa53ad386fb043',
			     '1800db8ae071d071eb02965296e476c0fc2554f8',
			     '5fe7615d92ae8ab9db99b14809eb0bddd06101cd', 	#Ks._Mario
			     'efa1facd3a57169608abf74e60c5b3a94990f50f', 	#Micky_Neilson,
			     'fa72a4f373b749ba4b1734d15621b7eab8cf4cff', 	#Anna_Onichimowska,
			     '4d07224ed7568e0451dce003d82788057f6465e9', 	#Krzysztof_Piskorski,
			     '9a8f6fe164878330844dc861ab2dd2c9701cda7c', 	#Daniel_Morgan_Perry
			     '05eeaea9702d728d14b9f269999f9dd2153b857a', 	#Food1.jpg
			     '955e7bfa2d908e4cfeebb12b17797d6239c8d2fd', 	#History_of_England//bits.wikimedia.org/static-1.22wmf9/skins/common/images/magnify-clip.png
			     'cd98c10bb00717d250878d09d4212367c734eb96', 	#History_of_England//bits.wikimedia.org/static-1.22wmf9/skins/common/images/poweredby_mediawiki_88x31.png
			     'f1cea642e49bebc097f4e57db88d287e60c0e0fa', 	#History_of_England//bits.wikimedia.org/static-1.22wmf9/skins/vector/images/search-ltr.png
	
				]
	elif lang=='zh':
		SpamList= [ 'bb7d64d96a472fd27d27036fb2f8a9e6e2757ac5', 	#%B2%A8%CCm%91%F0%D2%DB_Invasion_of_Poland
			    '916203e6d9cc86aa5949f6e07f75bdfb277a8055', 	#%E7%98%9C%EF%BC%BA%98%E5%89%9C%88%E5%95%A3%E8%9D%B4_Invasion_of_Poland
			    'e429e87d975fa87b95a09ae027b7ecab40be9414', 	#Wikipedia:\xE9\xA6\x96\xE9\xA1\xB5
			    '18fb3fbdc60b903e63d1f10511766162dfe1362b', 	#\xE5\xB0\x8F\xE9\x8E\xAE\xE6\x9C\x89\xE4\xBD\xA0
			    '731440e131dba173e18c2f782ec5b8dae23f0e06', 	#\xE6\x96\xB0\xE4\xB8\x96\xE7\xBA\xAA\xE7\xA6\x8F\xE9\x9F\xB3\xE6\x88\x98\xE5\xA3\xAB
			    '5923ccc3751d3c15352f7c7e35ab9f46fa4d3db3', 	#\xE5\xB8\xAB\xE7\x88\xB6\xC2\xB7\xE6\x98\x8E\xE7\x99\xBD\xE4\xBA\x86
			    'ce75a71d3c3e918373c9f6bfe04b0fd1624aeff9', 	#\xE6\xAD\xA5\xE5\x85\xB5
			    'db2268352b367a766524b9d7880b7c28c072ff70', 	#\xE5\xA4\xA9\xE7\xA9\xBA\xE4\xB9\x8B\xE5\x9F\x8E
			    '08284665fb7989cd27cfcd293e9973ca40a4308f', 	#\xE6\xA7\x8D\xE5\xBD\x88\xE8\xBE\xAF\xE9\xA7\x81_\xE5\xB8\x8C\xE6\x9C\x9B\xE5\xAD\xB8\xE5\x9C\x92\xE8\x88\x87\xE7\xB5\x95\xE6\x9C\x9B\xE9\xAB\x98\xE4\xB8\xAD\xE7\x94\x9F
			    'f7e09ade2a4255b14cb1e784e3d1140c2eb87ea2', 	#\xE7\xA5\x9E\xE4\xB8\x8D\xE5\x9C\xA8\xE7\x9A\x84\xE6\x98\x9F\xE6\x9C\x9F\xE5\xA4\xA9
			    '16159b16df13f78531247b088c6a8e8e68291cdf', 	#\xE5\xBC\xB5\xE5\x8F\x8B\xE9\xA9\x8A
			    '97907b5ccd95ed6baff7af688e21542b4306492d', 	#\xE5\xAE\xAB\xE5\xB4\x8E\xE9\xAA\x8F
			    '99f2fbe07457b73afa67f456facbce722543df1c', 	#\xE8\x91\xA3\xE6\xB5\xB7\xE5\xB7\x9D
			    'ad8bba50d9bf8370cce1938424f3a93e83dd80ee', 	#\xE5\x90\xB3\xE9\x8E\xAE\xE5\xAE\x87
			    'deddcf30a5ec0310984a8f0d2f717737ac7d0fa9', 	#\xE6\xB5\xB7\xE6\xBD\xAE\xE4\xB9\x8B\xE5\xA3\xB0
			    '07d262d685c0b8a75f23d2e050632bb66812e2e6', 	#\xE5\x8A\x89\xE5\xBE\xB7\xE8\x8F\xAF
			    '3c1a66d93bd4b230b7d6ec3cf3628f51446f82cb', 	#\xE5\x8F\xB2\xE4\xB8\x8A\xE6\x9C\x80\xE7\x89\x9B\xE9\x87\x98\xE5\xAD\x90\xE6\x88\xB6\xE4\xBA\x8B\xE4\xBB\xB6
			    '513881f71a24bff9908fd9214e8655341c63fdbb', 	#/bits.wikimedia.org/favicon/wikipedia.ico
			    '2f491f581e2c038620aa694cd47e9e07f3599940', 	#\xE9\xAD\x94\xE5\xA5\x87\xE5\xB0\x91\xE5\xB9\xB4
			    '121d5a59069e7c9e87e7c16226e0440a9c65e438', 	#\xE6\x9D\x8E\xE8\x81\x96\xE5\x82\x91
			    'b444fc393ed2e212c37e00a194388649c4f143d1', 	#\xE7\x9F\xB3\xE5\xB7\x9D\xE6\xA2\xA8\xE8\x8F\xAF
			    '48b208987bdbb4c27825c2b05e002f6f5ae005af', 	#\xE6\xB1\x9F\xE7\xA5\x96\xE5\xB9\xB3
				]
	elif lang=='ru':
		SpamList= [ 'f2f7f4438f0c496f601bab3f6938cd3cfb4c6052', 	#AC/edit
			    '99d692c55a080187cf0770cdf5c029d5ba98e048', 	#The_Genius/edit
			    '62e958d59133210768510e2f0fa0668a1c4dfd7d', 	#%D0%9A%D1%83%D0%B1%D0%BE%D0%BA_%D0%A3%D0%95%D0%A4%D0%90_2008/edit
			    '33aa66b4118532b358b9b200d2fd6362156f20c3', 	#\xD0\x97\xD0\xB0\xD0\xB2\xD0\xBE\xD1\x80\xD0\xBE\xD1\x82\xD0\xBD\xD1\x8E\xD0\xBA,_\xD0\x90\xD0\xBD\xD0\xB0\xD1\x81\xD1\x82\xD0\xB0\xD1\x81\xD0\xB8\xD1\x8F_\xD0\xAE\xD1\x80\xD1\x8C\xD0\xB5\xD0\xB2\xD0\xBD\xD0\xB0
			    'a34ffb7333053250c9e5146687895bd44a312808', 	#\xD0\xA4\xD1\x83\xD1\x82\xD1\x83\xD1\x80\xD0\xB0\xD0\xBC\xD0\xB0
			    '8e155e759f27e5fb1cdfe32bab05aae8d2051031', 	#%D0%A7%D0%B5%D0%BC%D0%BF%D0%B8%D0%BE%D0%BD%D0%B0%D1%82_%D0%A4%D1%80%D0%B0%D0%BD%D1%86%D0%B8%D0%B8_%D0%BF%D0%BE_%D1%84%D1%83%D1%82%D0%B1%D0%BE%D0%BB%D1%83_2011/edit
			    'bcb93d2eba04a93337028cbe202d6e3667937394', 	#%D0%9B%D0%B8%D0%B3%D0%B0_%D0%95%D0%B2%D1%80%D0%BE%D0%BF%D1%8B_%D0%A3%D0%95%D0%A4%D0%90_2013/edit
			    '1ab3790e89745555e406ed9f820196ca46360736', 	#\xD0\x97\xD0\xB0\xD0\xB3\xD0\xBB\xD0\xB0\xD0\xB2\xD0\xBD\xD0\xB0\xD1\x8F_\xD1\x81\xD1\x82\xD1\x80\xD0\xB0\xD0\xBD\xD0\xB8\xD1\x86\xD0\xB0
			    '9dccd42cf42c25d19e15373d82235fd7c4ee5335', 	#\xD0\x90\xD0\xB4\xD0\xB5\xD0\xBB\xD1\x8C\xD0\xB3\xD0\xB5\xD0\xB9\xD0\xBC,_\xD0\x9F\xD0\xB0\xD0\xB2\xD0\xB5\xD0\xBB_\xD0\x90\xD0\xBD\xD0\xB0\xD1\x82\xD0\xBE\xD0\xBB\xD1\x8C\xD0\xB5\xD0\xB2\xD0\xB8\xD1\x87
				]

	else:
		SpamList= []

	return SpamList

def fnLaunchNextJob(CURJOBNAME):
       if CURJOBNAME=='p0_dl' and fnIsPrevJobDone(CURJOBNAME):
               syslog.syslog('Launching split proc...p1_split')
               os.system('/usr/bin/python /home/ec2-user/Python/p1_split.py')          
       elif CURJOBNAME=='p1_split' and fnIsPrevJobDone(CURJOBNAME):
               syslog.syslog('Launching filter proc...p2_filter')
               os.system('/usr/bin/python /home/ec2-user/Python/p2_filter.py')                 
       elif CURJOBNAME=='p2_filter' and fnIsPrevJobDone(CURJOBNAME):
               syslog.syslog('Launching Russian filter...p2_ru_filter')
               os.system('/usr/bin/python /home/ec2-user/Python/p2_ru_filter.py')                
       elif CURJOBNAME=='p2_ru_filter' and fnIsPrevJobDone(CURJOBNAME):
               syslog.syslog('Launching move to action proc...p2x_move_to_action')
               os.system('/usr/bin/python /home/ec2-user/Python/p2x_move_to_action.py')                
       elif CURJOBNAME=='p2x_move_to_action' and fnIsPrevJobDone(CURJOBNAME):
               syslog.syslog('Launching add to db proc...p3_add_to_db')
               os.system('/usr/bin/python /home/ec2-user/Python/p3_add_to_db.py')              
       elif CURJOBNAME=='p3_add_to_db' and fnIsPrevJobDone(CURJOBNAME):
               syslog.syslog('Launching Image add to db...p3_image_add')
               os.system('/usr/bin/python /home/ec2-user/Python/p3_image_add_to_db.py')                
       elif CURJOBNAME=='p3_image_add' and fnIsPrevJobDone(CURJOBNAME):
               syslog.syslog('Launching Category add to db ...p3_category_add')
               os.system('/usr/bin/python /home/ec2-user/Python/p3_category_add_to_db.py')             
       elif CURJOBNAME=='p3_category_add' and fnIsPrevJobDone(CURJOBNAME):
               syslog.syslog('Launching p70 Export to csv file ...p70_export')
               os.system('/usr/bin/python /home/ec2-user/Python/p70_export.py')                
       elif CURJOBNAME=='p70_export' and fnIsPrevJobDone(CURJOBNAME):
               syslog.syslog('Launching p90 noise removal script ...p90_remove_noise')
               os.system('/usr/bin/python /home/ec2-user/Python/p90_remove_noise.py')          
       elif CURJOBNAME=='p90_remove_noise' and fnIsPrevJobDone(CURJOBNAME):
               syslog.syslog('Launching Mongo CSV Sort script ...sortMongoHD')
               os.system('/usr/bin/python /home/ec2-user/Python/sort_MongoHD.py')              
               os.system('/usr/bin/python /home/ec2-user/Python/sort_MongoHD.py')              
       elif CURJOBNAME=='sortMongoHD' and fnIsPrevJobDone(CURJOBNAME):
               syslog.syslog('Launching Tophits script ...tophits')
               os.system('/usr/bin/python /home/ec2-user/Python/tophits.py')           
       elif CURJOBNAME=='tophits' and fnIsPrevJobDone(CURJOBNAME):
               syslog.syslog('Launching ThreeHourRollingAvg script ...threehrrollingavg')
               os.system('/usr/bin/python /home/ec2-user/Python/genPages/threehourrollingavg.py')              
       elif CURJOBNAME=='threehrrollingavg' and fnIsPrevJobDone(CURJOBNAME):
               syslog.syslog('Launching prepop_filltmpHot script ...fillTmpHot')
               os.system('/usr/bin/python /home/ec2-user/Python/genPages/prepop_filltmpHot.py')                
       elif CURJOBNAME=='fillTmpHot' and fnIsPrevJobDone(CURJOBNAME):
               syslog.syslog('Launching populate_trending script ...populate_trending')
               os.system('/usr/bin/python /home/ec2-user/Python/genPages/populate_trending.py')                
       elif CURJOBNAME=='populate_trending' and fnIsPrevJobDone(CURJOBNAME):
               syslog.syslog('Launching populate_debuts script ...populate_debuts')
               os.system('/usr/bin/python /home/ec2-user/Python/genPages/populate_debuts.py')          
       elif CURJOBNAME=='populate_debuts' and fnIsPrevJobDone(CURJOBNAME):
               syslog.syslog('Launching populate_cold script ...populate_cold')
               os.system('/usr/bin/python /home/ec2-user/Python/genPages/populate_cold.py')            
       elif CURJOBNAME=='populate_cold' and fnIsPrevJobDone(CURJOBNAME):
               syslog.syslog('Launching populate_category script ...populate_category')
               os.system('/usr/bin/python /home/ec2-user/Python/genPages/populate_category.py')                
       elif CURJOBNAME=='populate_category' and fnIsPrevJobDone(CURJOBNAME):
               syslog.syslog('Launching populate_image script ...populate_image')
               os.system('/usr/bin/python /home/ec2-user/Python/genPages/populate_image.py')           
       elif CURJOBNAME=='populate_image' and fnIsPrevJobDone(CURJOBNAME):
               syslog.syslog('All Done with adding info to databae!')

       return
def fnGetStatusMsg(COLLCHECK):
       RECORD=db.logSystem.find_one({table:COLLCHECK})
       
       return RECORD['status']
def fnStartTimer():
       return time.time()
def fnEndTimerCalcRuntime(a):
       b=time.time()
       c=b-a
       d=round(c,3)
       return d
def fnSetStatusMsg(COLLCHECK,msgNum):
       DAY,MONTH,YEAR,HOUR,expiretime=fnReturnTimes()
       HOUR=minusHour(int(HOUR))
       HOUR=adjustHour(int(HOUR))
       QREC={'table':COLLCHECK}
       if msgNum==0:
               REC={'table':COLLCHECK,'mesg':'NOT Done'}
               db.logSystem.remove(QREC)
               db.logSystem.insert(REC)
       elif msgNum==1:
               REC={'table':COLLCHECK,'mesg':'Done'}
               db.logSystem.remove(QREC)
               db.logSystem.insert(REC)
       elif msgNum==3:
               REC={'table':COLLCHECK,'mesg':HOUR}
               db.logSystem.remove(QREC)
               db.logSystem.insert(REC)                
       return
def fnWaitForStatus(COLLCHECK):
       QREC={'table':COLLCHECK}
       REC=db.logSystem.find_one(QREC)
       if REC['mesg'] == 'NOT Done':
               time.sleep(5)
       return
def fnGetMonthName():
       return datetime.datetime.now().strftime("%B")

def fnReturnTimes():
        TODAY=date.today()
        YEAR=TODAY.year
        DAY=TODAY.day
        MONTH=TODAY.month
        HOUR=time.strftime('%H')
        now=datetime.datetime.now()
        half=now+datetime.timedelta(minutes=45)
        stamp=time.mktime(half.timetuple())
        expiretime=format_date_time(stamp)
        if int(HOUR) < 8:
               DAY-=1
        if DAY==0:
           DAY=30
           MONTH-=1
        if MONTH==0:
           DAY=31
           MONTH=12
           YEAR-=1
        return DAY, MONTH, YEAR,HOUR, expiretime


def fnFormatTimes(DAY,MONTH,HOUR):
       HOUR='%02d' % (int(HOUR),)
       DAY='%02d' % (int(DAY),)
       MONTH='%02d' % (int(MONTH),)

       return DAY,MONTH,HOUR
def fnFormatHour(HOUR):
	HOUR='%02d' % (int(HOUR),)
	return HOUR
def fnReturnTimeString(DAY,MONTH,YEAR):
	YEAR='%02d' % (int(YEAR),)
	DAY='%02d' % (int(DAY),)
	MONTH='%02d' % (int(MONTH),)
	return str(YEAR)+"_"+str(MONTH)+"_"+str(DAY)
def FormatName(title):
        s_title=string.replace(title,'_',' ')
        t_title=s_title.encode('utf-8')
        utitle=urllib2.unquote(t_title)
	try:
		utitle=utitle.decode('utf-8')
	except:
		utitle=title
        return title, utitle

def adjustHour(HOUR):
	if HOUR==-1:
		HOUR=23
	elif HOUR==-2:
		HOUR=22
	return HOUR

def minusHour(HOUR):
	HOUR-=1
	if HOUR==-1:
		HOUR=23
	elif HOUR==-2:
		HOUR=22
	elif HOUR==-3:
		HOUR=21
	elif HOUR==-4:
		HOUR=20
	elif HOUR==-5:
		HOUR=19
	elif HOUR==-6:
		HOUR=18
	elif HOUR==-7:
		HOUR=17
	return HOUR

def fnReturnLastThreeHours(HOUR):
	a=HOUR-1
	b=HOUR-2
	c=HOUR-3
	if a==-1:
		a=23
	if b==-1:
		b=23
	elif b==-2:
		b=22
	if c<0:
		c+=24
	return a,b,c
def fnStrFmtDate(DVAR):
	NEWVAR='%02d' % (DVAR,)	
	return NEWVAR

def toSyslog(msg):
    syslog.syslog(msg)

def PreviousDay(YEAR,MONTH,DAY):
    yd=DAY-1
    ym=MONTH
    yy=YEAR
    if yd==0:
        yd=30
        ym=MONTH-1
        if ym==0:
            ym=12
            yy=YEAR-1
    return yy,ym,yd
