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
leaderboard_account_file='*****/leaderboard_account.txt'

conn = psycopg2.connect(host= '192.168.0.***',
                        user='postgres',
                        password='*****',
                        database='indb',
                        port='5432',
                        )           
cursor = conn.cursor()


#Upload new data to temp_database

cursor.execute('''
CREATE temporary TABLE temp_leaderboard_account (
	id int8 NOT NULL,
	"name" varchar(50) NULL,
	handle varchar(50) NULL,
	"profileImage" varchar(512) NULL,
	"subscriberCount" int8 NULL,
	url varchar(100) NULL,
	platform varchar(50) NULL,
	"platformId" int8 NULL,
	"accountType" varchar(50) NULL,
	"pageAdminTopCountry" varchar(50) NULL,
	"pageDescription" varchar(128) NULL,
	"pageCreatedDate" varchar(128) NULL,
	"pageCategory" varchar(50) NULL,
	verified bool NULL
);
''')


#Insert new data to temp_account
f = open(leaderboard_account_file, 'r',encoding='utf-8')
next(f)
cursor.copy_from(f, 'temp_leaderboard_account', sep='|')
cursor.execute('ALTER TABLE temp_leaderboard_account ADD COLUMN create_time_holder TIMESTAMP without time zone NULL;')

cursor.execute('ALTER TABLE temp_leaderboard_account ALTER COLUMN "pageCreatedDate" TYPE TIMESTAMP without time zone USING create_time_holder;')
cursor.execute('update temp_leaderboard_account set "platformId" = null where "platformId"=0 ')
# If there is change in record update if there is no record insert into account table
cursor.execute('''
INSERT INTO leaderboard_account (id,"name",handle,"profileImage","subscriberCount",url,platform,"platformId","accountType","pageAdminTopCountry","pageDescription","pageCreatedDate","pageCategory",verified)
SELECT id,"name",handle,"profileImage","subscriberCount",url,platform,"platformId","accountType","pageAdminTopCountry","pageDescription","pageCreatedDate","pageCategory",verified
FROM temp_leaderboard_account
ON CONFLICT (id) DO UPDATE
SET id = EXCLUDED.id,
"name" = EXCLUDED."name",
"profileImage" = EXCLUDED."profileImage",
"subscriberCount" = EXCLUDED."subscriberCount",
url = EXCLUDED.url,
platform = EXCLUDED.platform,
"platformId" = EXCLUDED."platformId",
"accountType" = EXCLUDED."accountType",
"pageAdminTopCountry" = EXCLUDED."pageAdminTopCountry",
"pageDescription" = EXCLUDED."pageDescription",
"pageCreatedDate" = EXCLUDED."pageCreatedDate",
"pageCategory" = EXCLUDED."pageCategory",
verified = EXCLUDED.verified;
''')

# Commit changes to the database
conn.commit() 

# Close the database connection
conn.close()
