import psycopg2
import os
import datetime
import time
import pandas as pd

today = datetime.date.today().strftime("%Y%m%d")
currunttime = time.time()
current_datetime = datetime.datetime.now()
current_datetime_str = current_datetime.strftime("%Y-%m-%d_%H-%M-%S")
print(current_datetime_str)


#Paramenter & Definations
media_file='C:/Users/K3nXz/Desktop/sph/media.txt'

conn = psycopg2.connect(host= 'localhost',
                        user='postgres',
                        password='postgres',
                        database='indb',
                        port='5432',
                        )           
cursor = conn.cursor()


#Upload new data to temp_database
cursor.execute('''
create TEMPORARY table temp_media(

captionid INT,
post_id bigint,
"type" VARCHAR,
url VARCHAR,
height VARCHAR,
width VARCHAR ,
"full" VARCHAR,
media_id INT)
;
''')


#Insert new data to temp_account
f = open(media_file, 'r',encoding='utf-8')
next(f)
cursor.copy_from(f, 'temp_media', sep='|')


# If there is change in record update if there is no record insert into statistic table
cursor.execute('''
INSERT INTO media (captionid, post_id, "type", url, height, width, "full",media_id)
SELECT captionid, post_id, "type", url, height, width, "full",media_id
FROM temp_media
ON CONFLICT (captionid,post_id,media_id) DO UPDATE
SET captionid = EXCLUDED.captionid,
post_id = EXCLUDED.post_id,
media_id = EXCLUDED.media_id,
"type" = EXCLUDED. "type",
url = EXCLUDED.url,
height = EXCLUDED.height,
width = EXCLUDED.width,
"full" = EXCLUDED."full";
''')

# Commit changes to the database
conn.commit() 

# Close the database connection
conn.close()