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
statistic_file='/home/kenny/txt/statistic.txt'

conn = psycopg2.connect(host= '192.168.0.189',
                        user='postgres',
                        password='postgres',
                        database='indb',
                        port='5432',
                        )           
cursor = conn.cursor()


#Upload new data to temp_database
cursor.execute('''CREATE TEMPORARY TABLE temp_statistic (
    platformid varchar,
    facebookid bigint,
    postid bigint,
    "actual_likeCount" INTEGER,
    "actual_shareCount" INTEGER,
    "actual_commentCount" INTEGER,
    "actual_loveCount" INTEGER,
    "actual_wowCount" INTEGER,
    "actual_hahaCount" INTEGER,
    "actual_sadCount" INTEGER,
    "actual_angryCount" INTEGER,
    "actual_thankfulCount" INTEGER,
    "actual_careCount" INTEGER,
    "expected_likeCount" INTEGER,
    "expected_shareCount" INTEGER,
    "expected_commentCount" INTEGER,
    "expected_loveCount" INTEGER,
    "expected_wowCount" INTEGER,
    "expected_hahaCount" INTEGER,
    "expected_sadCount" INTEGER,
    "expected_angryCount" INTEGER,
    "expected_thankfulCount" INTEGER,
    "expected_careCount" INTEGER)''')


#Insert new data to temp_account
f = open(statistic_file, 'r',encoding='utf-8')
next(f)
cursor.copy_from(f, 'temp_statistic', sep='|')


# If there is change in record update if there is no record insert into statistic table
cursor.execute('''
INSERT INTO statistic (platformid, facebookid, postid, "actual_likeCount", "actual_shareCount", "actual_commentCount", "actual_loveCount", "actual_wowCount", "actual_hahaCount", "actual_sadCount", "actual_angryCount", "actual_thankfulCount", "actual_careCount", "expected_likeCount", "expected_shareCount", "expected_commentCount", "expected_loveCount", "expected_wowCount", "expected_hahaCount", "expected_sadCount", "expected_angryCount", "expected_thankfulCount", "expected_careCount")
SELECT platformid, facebookid, postid, "actual_likeCount", "actual_shareCount", "actual_commentCount", "actual_loveCount", "actual_wowCount", "actual_hahaCount", "actual_sadCount", "actual_angryCount", "actual_thankfulCount", "actual_careCount", "expected_likeCount", "expected_shareCount", "expected_commentCount", "expected_loveCount", "expected_wowCount", "expected_hahaCount", "expected_sadCount", "expected_angryCount", "expected_thankfulCount", "expected_careCount"
FROM temp_statistic
ON CONFLICT (facebookid, postid) DO UPDATE
SET "actual_likeCount" = EXCLUDED."actual_likeCount",
    "actual_shareCount" = EXCLUDED."actual_shareCount",
    "actual_commentCount" = EXCLUDED."actual_commentCount",
    "actual_loveCount" = EXCLUDED."actual_loveCount",
    "actual_wowCount" = EXCLUDED."actual_wowCount",
    "actual_hahaCount" = EXCLUDED."actual_hahaCount",
    "actual_sadCount" = EXCLUDED."actual_sadCount",
    "actual_angryCount" = EXCLUDED."actual_angryCount",
    "actual_thankfulCount" = EXCLUDED."actual_thankfulCount",
    "actual_careCount" = EXCLUDED."actual_careCount",
    "expected_likeCount" = EXCLUDED."expected_likeCount",
    "expected_shareCount" = EXCLUDED."expected_shareCount",
    "expected_commentCount" = EXCLUDED."expected_commentCount",
    "expected_loveCount" = EXCLUDED."expected_loveCount",
    "expected_wowCount" = EXCLUDED."expected_wowCount",
    "expected_hahaCount" = EXCLUDED."expected_hahaCount",
    "expected_sadCount" = EXCLUDED."expected_sadCount",
    "expected_angryCount" = EXCLUDED."expected_angryCount",
    "expected_thankfulCount" = EXCLUDED."expected_thankfulCount",
    "expected_careCount" = EXCLUDED."expected_careCount";
''')

# Commit changes to the database
conn.commit() 

# Close the database connection
conn.close()