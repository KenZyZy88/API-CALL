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
account_file='/home/kenny/txt/account.txt'

conn = psycopg2.connect(host= '192.168.0.200',
                        user='postgres',
                        password='postgres',
                        database='indb',
                        port='5432',
                        )           
cursor = conn.cursor()


#Upload new data to temp_database

cursor.execute('''CREATE TEMPORARY TABLE temp_account
(captionid int4 NULL,name varchar(50) NULL,handle varchar(50) NULL,profileimage varchar(512) NULL,subscribercount int4 NULL,url varchar(50) NULL,
platform varchar(50) NULL,platformid int8 NULL,accounttype varchar(50) NULL,pageadmintopcountry varchar(50) NULL,verified bool NULL)''')

#Insert new data to temp_account
f = open(account_file, 'r',encoding='utf-8')
next(f)
cursor.copy_from(f, 'temp_account', sep='|')


# If there is change in record update if there is no record insert into account table
cursor.execute('''
INSERT INTO account (captionid, name, handle, profileImage, subscriberCount, url, platform, platformId, accountType, pageAdminTopCountry, verified)
SELECT captionid, name, handle, profileImage, subscriberCount, url, platform, platformId, accountType, pageAdminTopCountry, verified
FROM temp_account
ON CONFLICT (captionid) DO UPDATE
SET name = EXCLUDED.name,
handle = EXCLUDED.handle,
profileImage = EXCLUDED.profileImage,
subscriberCount = EXCLUDED.subscriberCount,
url = EXCLUDED.url,
platform = EXCLUDED.platform,
platformId = EXCLUDED.platformId,
accountType = EXCLUDED.accountType,
pageAdminTopCountry = EXCLUDED.pageAdminTopCountry,
verified = EXCLUDED.verified;
''')



# Commit changes to the database
conn.commit() 

# Close the database connection
conn.close()