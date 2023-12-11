import psycopg2
import os
import datetime
import time
import pandas as pd

# Get the current date and time
today = datetime.date.today().strftime("%Y%m%d")
currunttime = time.time()
current_datetime = datetime.datetime.now()
current_datetime_str = current_datetime.strftime("%Y-%m-%d_%H-%M-%S")
print(current_datetime_str)

#Paramenter & Definations
# account_file='/home/kenny/txt/account.txt' #Linux
leaderboard_summary_file='/home/kenny/txt/leaderboard_subscriberData.txt'

conn = psycopg2.connect(host= '192.168.0.200',
                        user='postgres',
                        password='postgres',
                        database='indb',
                        port='5432',
                        )           
cursor = conn.cursor()


#Upload new data to temp_database

cursor.execute('''
CREATE temporary TABLE temp_leaderboard_subscriberdata (
	id int8 NOT NULL,
	"name" varchar(50) NULL,
	"date" varchar(50) NULL,
	"initialCount" int8 NULL,
	"finalCount" int8 NULL,
	notes varchar(255) NULL
);

''')






#Insert new data to temp_account
f = open(leaderboard_summary_file, 'r',encoding='utf-8')
next(f)
cursor.copy_from(f, 'temp_leaderboard_subscriberdata', sep='|')


# If there is change in record update if there is no record insert into account table
cursor.execute('''
INSERT INTO leaderboard_subscriberdata (id,name,date,"initialCount","finalCount",notes)
SELECT id,name,date,"initialCount","finalCount",notes
FROM temp_leaderboard_subscriberdata
ON CONFLICT (id) DO UPDATE
SET id = EXCLUDED.id,
name = EXCLUDED.name,
date = EXCLUDED.date,
"initialCount" = EXCLUDED."initialCount",
"finalCount" = EXCLUDED."finalCount",
"notes" = EXCLUDED."notes";
''')

# Commit changes to the database
conn.commit() 

# Close the database connection
conn.close()