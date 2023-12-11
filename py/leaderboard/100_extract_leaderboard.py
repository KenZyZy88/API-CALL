import pandas as pd
import requests
from datetime import datetime
import pytz
import os
import json
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)

#LocalTime convertion to UTC-0
local_time = datetime.now()
local_tz = pytz.timezone('Asia/Kuala_Lumpur')
local_time = local_tz.localize(local_time)
gmt_time = local_time.astimezone(pytz.utc)
date_str = gmt_time.strftime('%Y-%m-%d')

# Make the API call and load to pandas
response = requests.get('https://api.crowdtangle.com/leaderboard?token=wWJYBmB2VTRLhAEVhMe8UOjTTcphXLw2CDREOwdC')
DataReceieve  = response.json()["result"]
PostReceive = DataReceieve["accountStatistics"]
ingest_table= pd.DataFrame(PostReceive)

#Create table in proper format
read_table = pd.DataFrame(columns=['account','summary','breakdown','subscriberData'])
read_table['account'] = ingest_table['account']
read_table['summary'] = ingest_table['summary']
read_table['breakdown'] = ingest_table['breakdown']
read_table['subscriberData'] = ingest_table['subscriberData']


def leaderboard_table():

    if os.path.exists('/home/kenny/txtleaderboard.txt'):
        read_table.to_csv('/home/kenny/txtleaderboard.txt', sep='|', index=False,header=True)
    else:
        read_table.to_csv('/home/kenny/txt/leaderboard.txt', sep='|', index=False,header=True)


def account_table():
    account_table = pd.DataFrame(columns=['id','name','handle','profileImage','subscriberCount','url','platform','platformId','accountType','pageAdminTopCountry','pageDescription','pageCreatedDate','pageCategory','verified'])
    
    for x in range(0, len(ingest_table)):

        df = ingest_table['account'][x]
        account_table.loc[x] = [df['id'], df['name'], df['handle'], df['profileImage'], df['subscriberCount'], df['url'], df['platform'], df.get('platformId', ''), df['accountType'], df.get('pageAdminTopCountry', ''), df.get('pageDescription', ''), df.get('pageCreatedDate', ''), df.get('pageCategory', ''), df.get('verified', '')]
        if os.path.exists('/home/kenny/txt/leaderboard_account.txt'):
            account_table.to_csv('/home/kenny/txt/leaderboard_account.txt', sep='|', index=False,header=True)
        else:
            account_table.to_csv('/home/kenny/txt/leaderboard_account.txt', sep='|', index=False,header=True)

def summary_table():
    summary_table = pd.DataFrame(columns=['id','name','date', 'loveCount', 'totalInteractionCount', 'wowCount', 'thankfulCount', 'interactionRate', 'likeCount', 'hahaCount', 'commentCount', 'shareCount', 'careCount','sadCount', 'angryCount', 'totalVideoTimeMS', 'postCount'])
    
    for x in range(0, len(ingest_table)): 
        df = ingest_table['summary'][x]
        id= ingest_table['account'][x]['id']
        name= ingest_table['account'][x]['name'] 
        # account_table.loc[x] = ['date': date_str,'loveCount':df['loveCount'],'totalInteractionCount':df['totalInteractionCount'],'wowCount':df['wowCount'], 'thankfulCount':df['thankfulCount'], 'interactionRate':df['interactionRate'], 'likeCount':df['likeCount'], 'hahaCount':df['hahaCount'], 'commentCount':df['commentCount'], 'shareCount':df['shareCount'], 'careCount':df['careCount'], 'sadCount':df['sadCount'], 'angryCount':df['angryCount'], 'totalVideoTimeMS':df.get('totalVideoTimeMS',''), 'postCount':df['postCount']]
        # account_table.loc[x] = [date_str,df['loveCount'],df['totalInteractionCount'],df['wowCount'],df['thankfulCount'],df['interactionRate'],df['likeCount'],df['hahaCount'],df['commentCount'], df['shareCount'],df['careCount'], df['sadCount'], df['angryCount'], df.get('totalVideoTimeMS',''), df['postCount']]
        summary_table.loc[x] = [id,name,date_str,df.get('loveCount',''), df.get('totalInteractionCount',''), df.get('wowCount',''), df.get('thankfulCount',''), df.get('interactionRate',''), df.get('likeCount',''), df.get('hahaCount',''), df.get('commentCount',''), df.get('shareCount',''), df.get('careCount',''), df.get('sadCount',''), df.get('angryCount',''), df.get("totalVideoTimeMS",""), df.get('postCount',''),]
        if os.path.exists('/home/kenny/txt/leaderboard_summary.txt'):
            summary_table.to_csv('/home/kenny/txt/leaderboard_summary.txt', sep='|', index=False,header=True)
        else:
            summary_table.to_csv('/home/kenny/txt/leaderboard_summary.txt', sep='|', index=False,header=True)

def breakdown_table():
    breakdown_table = pd.DataFrame(columns=['id','name','date','media', 'loveCount', 'totalInteractionCount', 'wowCount', 'thankfulCount', 'interactionRate', 'likeCount', 'hahaCount', 'commentCount', 'shareCount', 'careCount','sadCount', 'angryCount', 'totalVideoTimeMS', 'postCount'])
    
    for x in range(0, len(ingest_table)):
        id= ingest_table['account'][x]['id']
        name= ingest_table['account'][x]['name']
        for media in ingest_table['breakdown'][x]:
            row = ingest_table['breakdown'][x][media]
            breakdown_table.loc[x] = [id,name,date_str,media,row.get('loveCount',''), row.get('totalInteractionCount',''), row.get('wowCount',''), row.get('thankfulCount',''), row.get('interactionRate',''), row.get('likeCount',''), row.get('hahaCount',''), row.get('commentCount',''), row.get('shareCount',''), row.get('careCount',''), row.get('sadCount',''), row.get('angryCount',''), row.get("totalVideoTimeMS",""), row.get('postCount',''),]

        if os.path.exists('/home/kenny/txt/leaderboard_breakdown.txt'):
            breakdown_table.to_csv('/home/kenny/txt/leaderboard_breakdown.txt', sep='|', index=False,header=True)
        else:
            breakdown_table.to_csv('/home/kenny/txt/leaderboard_breakdown.txt', sep='|', index=False,header=True)

def subscriberData_table():
    subscriberData_table = pd.DataFrame(columns=['id','name','date','initialCount','finalCount','notes'])
    for x in range(0, len(ingest_table)):
        id= ingest_table['account'][x]['id']
        name= ingest_table['account'][x]['name']
        df = ingest_table['subscriberData'][x]

        subscriberData_table.loc[x] = [id,name,date_str,df.get('initialCount',''), df.get('finalCount',''), df.get('notes','')]

        if os.path.exists('/home/kenny/txt/leaderboard_subscriberData.txt'):
            subscriberData_table.to_csv('/home/kenny/txt/leaderboard_subscriberData.txt', sep='|', index=False,header=True)
        else:
            subscriberData_table.to_csv('/home/kenny/txt/leaderboard_subscriberData.txt', sep='|', index=False,header=True)

#Classes to run

leaderboard_table()
account_table()
summary_table()
breakdown_table()
subscriberData_table()