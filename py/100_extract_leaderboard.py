import pandas as pd
import requests
from datetime import datetime
import pytz
import os
import json

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

    if os.path.exists('C:/Users/K3nXz/Desktop/sph/leaderboard.txt'):
        read_table.to_csv('C:/Users/K3nXz/Desktop/sph/leaderboard.txt', sep='|', index=False,header=True)
    else:
        read_table.to_csv('C:/Users/K3nXz/Desktop/sph/leaderboard.txt', sep='|', index=False,header=True)


def account_table():
    account_table = pd.DataFrame(columns=['id','name','handle','profileImage','subscriberCount','url','platform','platformId','accountType','pageAdminTopCountry','pageDescription','pageCreatedDate','pageCategory','verified'])
    
    for x in range(0, len(ingest_table)): 
        df = ingest_table['account'][x]
        account_table.loc[x] = [df['id'], df['name'], df['handle'], df['profileImage'], df['subscriberCount'], df['url'], df['platform'], df.get('platformId', ''), df['accountType'], df.get('pageAdminTopCountry', ''), df.get('pageDescription', ''), df.get('pageCreatedDate', ''), df.get('pageCategory', ''), df.get('verified', '')]
        if os.path.exists('C:/Users/K3nXz/Desktop/sph/leaderboard_account.txt'):
            account_table.to_csv('C:/Users/K3nXz/Desktop/sph/leaderboard_account.txt', sep='|', index=False,header=True)
        else:
            account_table.to_csv('C:/Users/K3nXz/Desktop/sph/leaderboard_account.txt', sep='|', index=False,header=True)

def summary_table():
    #Date
    #CurruntTime
    #id
    summary_table = pd.DataFrame(columns=[{'date','loveCount', 'totalInteractionCount', 'wowCount', 'thankfulCount', 'interactionRate', 'likeCount', 'hahaCount', 'commentCount', 'shareCount', 'careCount', 'sadCount', 'angryCount', 'totalVideoTimeMS', 'postCount'}])
    summary_table['date'] = date_str
# leaderboard_table()
# account_table()