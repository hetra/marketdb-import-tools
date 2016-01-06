import psycopg2
import csv

slug = "" # e.g. slug = "asx_qan"

timestamps = []
opens = []
highs = []
lows = []
closes = []
volumes = []

with open('table.csv', 'r') as csvfile:
	stockReader = csv.reader(csvfile, delimiter=',')
	next(stockReader)
	
	for row in stockReader:
		place = 0
		#print ', '.join(row)
		
		for cell in row:
			if place == 0:
				timestamps.append(cell)
			elif place == 1:
				opens.append(int(float(cell)*100))
			elif place == 2:
				highs.append(int(float(cell)*100))
			elif place == 3:
				lows.append(int(float(cell)*100))
			elif place == 4:
				closes.append(int(float(cell)*100))
			elif place == 5:
				volumes.append(int(cell))
			
			place += 1
		
				
#Define our connection string
conn_string = "dbname=equities user=jmcph4"

# print the connection string we will use to connect
print "Connecting to database\n	->%s" % (conn_string)
 
# get a connection, if a connect cannot be made an exception will be raised here
conn = psycopg2.connect(conn_string)
 
# conn.cursor will return a cursor object, you can use this cursor to perform queries
cursor = conn.cursor()
print "Connected!"

cursor.execute("CREATE TABLE IF NOT EXISTS {0} (id INTEGER NOT NULL PRIMARY KEY, timestamp TIMESTAMP WITH TIME ZONE NOT NULL, open INTEGER NOT NULL, high INTEGER NOT NULL, low INTEGER NOT NULL, close INTEGER NOT NULL, volume BIGINT NOT NULL);".format(slug))

for i in range(len(timestamps)):
	temp = (slug, 
	i+1,
	timestamps[i], 
	opens[i],
	highs[i],
	lows[i],
	closes[i],
	volumes[i])
	
	query = "INSERT INTO {0} (id, timestamp, open, high, low, close, volume) VALUES({1}, '{2}', {3}, {4}, {5}, {6}, {7});".format(*temp)
	
	#print query
	try:
		cursor.execute(query)
	except psycopg2.ProgrammingError:
		print "syntax error!"
		
conn.commit()
print "inserted {0} rows".format(len(timestamps))
