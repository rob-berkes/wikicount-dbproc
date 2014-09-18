wikicount-dbproc
================

Backend processing into MongoDB server.  With three hour rolling average and ton of test analysis scripts of wildly varying quality in /bin/

Crontab settings
================

15 * * * * /usr/bin/python /home/ubuntu/wikicount-dbproc/master.py
15 * * * * /usr/bin/python /home/ubuntu/wikicount-dbproc/collectMongostat.py
24 * * * * /usr/bin/python /home/ubuntu/wikicount-dbproc/threehourrollingavg.py
