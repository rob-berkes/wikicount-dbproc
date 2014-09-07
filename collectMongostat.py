import subprocess
import smtplib
from email.mime.text import MIMEText

OFILE=open('/tmp/mongostats.log','w')
subprocess.Popen(['/usr/bin/mongostat','-n','120','5'],stdout=OFILE)
OFILE.close()

EMFP = open('/tmp/mongostats.log','r')
msg = MIMEText(EMFP.read())
EMFP.close()

msg['Subject'] = 'MongoDB: mongostat logs for hourly Wikitrends.Info run'
msg['From'] = 'masterpy@wikitrends.info'
msg['To'] = 'rob.berkes@gmail.com'

s = smtplib.SMTP('localhost')
s.sendmail('masterpy@wikitrends.info','rob.berkes@gmail.com',msg.as_string())
s.quit()

