import MySQLdb
import csv
import sys

#Create connection to MySQL database
hostname = 'XXXX'
username = 'XXXX'
password = 'XXXX'
database = 'XXXX'
conn = MySQLdb.connect( host=hostname, user=username, passwd=password, db=database )
cur = conn.cursor()

#Provide the paths to the EEG log, your event marker file, and your session description file
dataFileName = input("Enter the data file path:  ")
eventFileName = input("Enter the event file path:  ")
sessionFilename = input("Enter the session loader file path:  ")
savePath = input("Enter the path to save your file to:  ")

#Define a dictionary of start/end codes and their associated experiments
startCodes = {'10' : 1, '20' : 2, '30' : 3, '40' : 4, '50' : 5}
endCodes = {'19' : 1, '29' : 2, '39' : 3, '49' : 4, '59' : 5}

#Determine the maximum Session ID and Record ID so we know where to start with new data. If no records exist, assign starting ID numbers
cur.execute("SELECT max(record_id), max(s.session_id) FROM SESSIONS s, RECORDS r where s.session_id = r.session_id")
maxRecordID, maxSessionID = cur.fetchall()[0]
if maxSessionID is None or maxRecordID is None:
	maxSessionID = 0
	maxRecordID = 1

#Open our session file and load data, inserting new Session IDs for each new row
sessionRecords = []
with open(sessionFilename, newline='') as inputfile:
	reader = csv.reader(inputfile)
	next(reader)
	for row in reader:
		maxSessionID += 1
		subject = row[0]
		sessionRecords.append([maxSessionID] + row + [sessionFilename])

#Open our EEG data file and load data, inserting some additional dummy columns that will be filled later
dataRecords = []
with open(dataFileName, newline='') as inputfile:
	reader = csv.reader(inputfile)
	next(reader)
	for row in reader:
		dataRecords.append([0, 0, subject, '99'] + [row[1],row[3],row[4],row[5],row[6]])

#Open our event file and load data
eventRecords = []
with open(eventFileName, newline='') as inputfile:
	reader = csv.reader(inputfile)
	for row in reader:
		eventRecords.append(row)
		
#Sort data on time column
dataRecords.sort(key=lambda x: x[4])
eventRecords.sort(key=lambda x: x[1])

#Find the row of data that is closest in time to each event marker, and assign the event marker to it
prevKey = 0
for eventRow in eventRecords:
	prevDifference = 'null'
	eventTime = eventRow[1]
	for dataRow in dataRecords[prevKey:]:
		dataTime = dataRow[4]
		difference = abs(float(eventTime) - float(dataTime))
		if prevDifference == 'null':
			pass
		elif difference < prevDifference:
			prevDifference = difference
		elif difference > prevDifference:
			dataRecords[prevKey - 1][3] = eventRow[0]
			break

		prevKey += 1
		prevDifference = difference

finalRecords = []
experimentID = 0	
sessCounter = 0	
rowCounter = 1
expStatus = "Null"

#For each line in dataRecords, we will match up all appropriate IDs and markers
for row in dataRecords:
	if row[3] in startCodes.keys():

		#Ensure that the start code that was matched to a data record is the same as what is in the sessions file
		if int(startCodes[row[3]]) != int(sessionRecords[sessCounter][2]):
			print("Critical Error: Start Code EXP_ID mismatch between Session Loader and marker files")
			sys.exit()
		
		#If a start code has already been identified, change its marker to 99 and consider the new start code to be the experiment beginning
		if expStatus == "Started":
			finalRecords[startRow][4] = '99'
			sessCounter -= 1
		
		#Assign Experiment IDs, change Experiment Status and increase our counters used elsewhere
		experimentID = startCodes[row[3]]
		sessCounter += 1
		expStatus = "Started"
		startRow = rowCounter
	
	#assign new values to row for Experiment and Session IDs
	row[0] = experimentID
	row[1] = maxSessionID - len(sessionRecords) + sessCounter
	
	#Only append data to finalRecords if it was in between start and end codes
	if experimentID != 0:
		finalRecords.append([maxRecordID + rowCounter] + row)
		rowCounter += 1
	
	#Change experiment status to ended if an end code is identified
	if row[3] in endCodes.keys():

		#Ensure that the end code that was matched to a data record is the same as what is in the sessions file
		if int(endCodes[row[3]]) != int(sessionRecords[sessCounter - 1][2]):
			print("Critical Error: End Code EXP_ID mismatch between Session Loader and marker files")
			sys.exit()
			
		experimentID = 0
		expStatus = "Ended"

#Check to make sure the count of start codes matched from events file is the same as what is in the sessions file
if sessCounter != len(sessionRecords):
	print("Invalid Session File! Counts from file don't match number of start codes")
else:
	with open(savePath, 'w', newline='') as csv_out:
		mywriter = csv.writer(csv_out)
		mywriter.writerows(finalRecords)

#Load final data to our SESSIONS and RECORDS tables		
cur.executemany("INSERT INTO SESSIONS (SESSION_ID,SUBJECT_ID,EXP_ID,BAD_DATA_FLAG,SITE_1_DESC,SITE_2_DESC,SITE_3_DESC,SITE_4_DESC,DATE,FILENAME) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", sessionRecords)
cur.executemany("INSERT INTO RECORDS (RECORD_ID,EXP_ID,SESSION_ID,SUBJECT_ID,EVENT,TIME,SITE_1,SITE_2,SITE_3,SITE_4) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE RECORD_ID=RECORD_ID,EXP_ID=EXP_ID,SESSION_ID=SESSION_ID,SUBJECT_ID=SUBJECT_ID,EVENT=EVENT,TIME=TIME,SITE_1=SITE_1,SITE_2=SITE_2,SITE_3=SITE_3,SITE_4=SITE_4", finalRecords)
conn.commit()