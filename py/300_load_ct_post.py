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
post_file='/home/kenny/txt/post.txt'

conn = psycopg2.connect(host= '192.168.0.189',
                        user='postgres',
                        password='postgres',
                        database='indb',
                        port='5432',
                        )           
cursor = conn.cursor()

#Upload new data to temp_database

cursor.execute('''
CREATE TEMPORARY TABLE temp_post (
    platformid varchar,
    platform varchar,
    date varchar ,
    updated varchar ,
    type varchar,
    title varchar,
    captionid int,
    caption varchar,
    description varchar,
    message varchar,
    expandedlinks varchar,
    link varchar,
    facebook_id bigint,
    post_id bigint,
    posturl varchar,
    subscribercount bigint,
    score  NUMERIC(20, 15),
    languagecode varchar,
    legacyid bigint,
    videolengthms varchar,
    imagetext varchar
);''')



#Insert new data to temp_account
f = open(post_file, 'r',encoding='utf-8')
next(f)

cursor.copy_from(f, 'temp_post', sep='|')

cursor.execute("ALTER TABLE temp_post ALTER COLUMN date TYPE timestamp USING date::timestamp without time zone;")
cursor.execute("ALTER TABLE temp_post ALTER COLUMN updated TYPE timestamp USING date::timestamp without time zone;")

# cursor.execute('select * from temp_post')

# rows = cursor.fetchall()

# for row in rows:
#     print(row)


cursor.execute('''
insert into post ( platformid , platform,date,updated,type,title,captionid,caption,description,message,expandedlinks,link,facebook_id,post_id,posturl,subscribercount,score,languagecode,legacyid,videolengthms,imagetext )
select  platformid,platform,date,updated,type,title,captionid,caption,description,message,expandedlinks,link,facebook_id,post_id,posturl,subscribercount,score,languagecode,legacyid,videolengthms,imagetext 
from temp_post
on conflict (captionid, post_id) do update
set platformid = excluded.platformid,
platform = excluded.platform,
date = excluded.date,
updated = excluded.updated,
type = excluded.type,
title = excluded.title,
caption = excluded.caption,
description = excluded.description,
message = excluded.message,
expandedlinks = excluded.expandedlinks,
link = excluded.link,
facebook_id = excluded.facebook_id,
posturl = excluded.posturl,
subscribercount = excluded.subscribercount,
score = excluded.score,
languagecode = excluded.languagecode,
legacyid = excluded.legacyid,
videolengthms = excluded.videolengthms,
imagetext = excluded.imagetext;''')


# Commit changes to the database
conn.commit() 

# Close the database connection
conn.close()