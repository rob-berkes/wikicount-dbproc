#/usr/bin/python
from pymongo import Connection 

conn=Connection()
db=conn.wc

#'8f9e9b397d590520d0938b3a063c1c0b58ba8445'  -- Main_Page , the #1 every day of course
#'ec15401c608667dbbb08a37856c815740a6a567e'  -- Wsearch.php #2
#'d5d4cd07616a542891b7ec2d0257b3a24b69856e' -- undefined
#'0646f4afd90c8fdb87bbcb57b63ee1911f5a9a46' -- Undefined
#'f6013a00b362253c64368d6eebc50ea2131754e2' -- index.html
#'f804b96e622b795446c20a7a910581fa096d2e1b' -- Main_Page/
#'dbf11f384ec9a5524a423af65e266627ae493d3c' -- 500.shtml
#'4f983a660ea1223340ae32159c1ab35d5e29bd2f' -- Wiki
HASHES=['8f9e9b397d590520d0938b3a063c1c0b58ba8445','ec15401c608667dbbb08a37856c815740a6a567e','d5d4cd07616a542891b7ec2d0257b3a24b69856e','0646f4afd90c8fdb87bbcb57b63ee1911f5a9a46','f6013a00b362253c64368d6eebc50ea2131754e2','f804b96e622b795446c20a7a910581fa096d2e1b','dbf11f384ec9a5524a423af65e266627ae493d3c','4f983a660ea1223340ae32159c1ab35d5e29bd2f']

for item in HASHES:
	db.hits.remove({'_id':item})
	db.map.remove({'_id':item})
	db.hitshourly.remove({'_id':item})

