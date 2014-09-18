from pymongo import Connection
conn=Connection()
db=conn.neuralweights

def printHourlyDifferences():
	initRS=db['init_settings'].find_one({'_id':'InitWikiHourlyWeights'})
	oldRS=db['old_settings'].find_one({'_id':'OldWikiHourlyWeights'})
	RS=db['settings'].find_one({'_id':'WikiHourlyWeights'})

	VALUES=RS['Values']

	try:
		oldVALUES=oldRS['Values']
		initVALUES=initRS['Values']
		print "Showing differences in hourly weights since last run and all time..."
		for a in range(0,23):
			print 'Hour: %d Diff Change: %.6f Total: %.6f' % (a,float(VALUES[a])-oldVALUES[a],float(VALUES[a])-initVALUES[a])
	except TypeError:
		print 'Old values do not exist. Transferring current values over. Please rerun after doing some more scoring'
		NEWREC={'_id':'InitWikiHourlyWeights','Values':VALUES}
		db['init_settings'].insert(NEWREC)

	NEWREC={'_id':'OldWikiHourlyWeights','Values':VALUES}
	db['old_settings'].insert(NEWREC)

def printDayDifferences():
	initRS=db['init_settings'].find_one({'_id':'InitWikiDayWeights'})
	oldRS=db['old_settings'].find_one({'_id':'OldWikiDayWeights'})
	RS=db['settings'].find_one({'_id':'WikiDayWeights'})
	
	VALUES=RS['Values']
#	try:
	oldVALUES=oldRS['Values']
	initVALUES=initRS['Values']
	print "Displaying differences in daily weights since last backup and in total."
	for a in range(1,15):
		print 'Day: %d Diff Change: %.6f Total: %.6f' % (a,float(VALUES[a])-oldVALUES[a],float(VALUES[a])-initVALUES[a])


	#NOW make cur values 'old' values
	NEWREC={'_id':'OldWikiDayWeights','Values':VALUES}
	db['old_settings'].insert(NEWREC)
	return

printDayDifferences()
printHourlyDifferences()
