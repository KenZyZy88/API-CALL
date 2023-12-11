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
leaderboard_summary_file='/home/kenny/txt/leaderboard_summary.txt'

conn = psycopg2.connect(host= '192.168.0.200',
                        user='postgres',
                        password='postgres',
                        database='indb',
                        port='5432',
                        )           
cursor = conn.cursor()


#Upload new data to temp_database

cursor.execute('''
CREATE temporary TABLE temp_leaderboard_summary (
	id int8 NOT NULL,
	"name" varchar(100) NULL,
	"date" date NULL,
	"loveCount" int8,
	"totalInteractionCount" int8,
	"wowCount" int8 NULL,
	"thankfulCount" int8 NULL,
	"interactionRate" float8 NULL,
	"likeCount" int8 NULL,
	"hahaCount" int8 NULL,
	"commentCount" int8 NULL,
	"shareCount" int8 NULL,
	"careCount" int8 NULL,
	"sadCount" int8 NULL,
	"angryCount" int8 NULL,
	"totalVideoTimeMS" varchar(50) NULL,
	"postCount" int8 NULL

);
''')






#Insert new data to temp_account
f = open(leaderboard_summary_file, 'r',encoding='utf-8')
next(f)
cursor.copy_from(f, 'temp_leaderboard_summary', sep='|')


# If there is change in record update if there is no record insert into account table
cursor.execute('''
INSERT INTO leaderboard_summary (id,name,date,"loveCount","totalInteractionCount","wowCount","thankfulCount","interactionRate","likeCount","hahaCount","commentCount","shareCount","careCount","sadCount","angryCount","totalVideoTimeMS","postCount")
SELECT id,name,date,"loveCount","totalInteractionCount","wowCount","thankfulCount","interactionRate","likeCount","hahaCount","commentCount","shareCount","careCount","sadCount","angryCount","totalVideoTimeMS","postCount"
FROM temp_leaderboard_summary
ON CONFLICT (id) DO UPDATE
SET id = EXCLUDED.id,
name = EXCLUDED.name,
date = EXCLUDED.date,
"loveCount" = EXCLUDED."loveCount",
"totalInteractionCount" = EXCLUDED."totalInteractionCount",
"wowCount" = EXCLUDED."wowCount",
"thankfulCount" = EXCLUDED."thankfulCount",
"interactionRate" = EXCLUDED."interactionRate",
"likeCount" = EXCLUDED."likeCount",
"hahaCount" = EXCLUDED."hahaCount",
"commentCount" = EXCLUDED."commentCount",
"shareCount" = EXCLUDED."shareCount",
"careCount" = EXCLUDED."careCount",
"sadCount" = EXCLUDED."sadCount",
"angryCount" = EXCLUDED."angryCount",
"totalVideoTimeMS" = EXCLUDED."totalVideoTimeMS",
"postCount" = EXCLUDED."postCount";
''')

# Commit changes to the database
conn.commit() 

# Close the database connection
conn.close()