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
response = requests.get('https://api.crowdtangle.com/posts?token=wWJYBmB2VTRLhAEVhMe8UOjTTcphXLw2CDREOwdC&count=50')
DataReceieve  = response.json()["result"]
PostReceive = DataReceieve["posts"]
ingest_table= pd.DataFrame(PostReceive)

#Create table in proper format
read_table = pd.DataFrame(columns=['platformId','platform','date','updated','type','title','captionid','caption','description','message','expandedLinks','link','facebook_id','post_id','postUrl','subscriberCount','score','media','statistics','account','languageCode','legacyId','id','videoLengthMS','imageText'])

#Transform Data - create facebook_id, post_id and captionid(publisher) for better search function
ingest_table[['facebook_id', 'post_id']] = ingest_table['platformId'].str.split('_', expand=True)
ingest_table['captionid'] = [row.split('|')[0] for row in ingest_table['id']]





#Ingest data to read_table - move to NoSQL if have time
read_table['platformId'] = ingest_table['platformId']
read_table['platform'] = ingest_table['platform']
read_table['date'] = ingest_table['date']
read_table['updated'] = ingest_table['updated']
read_table['type'] = ingest_table['type']
read_table['post_id'] = ingest_table['post_id']
read_table['title'] = ingest_table['title']
read_table['captionid'] = ingest_table['captionid']
read_table['caption'] = ingest_table['caption']
read_table['description'] = ingest_table['description']
read_table['message'] = ingest_table['message']
read_table['expandedLinks'] = ingest_table['expandedLinks']
read_table['link'] = ingest_table['link']
read_table['facebook_id'] = ingest_table['facebook_id']
read_table['postUrl'] = ingest_table['postUrl']
read_table['subscriberCount'] = ingest_table['subscriberCount']
read_table['score'] = ingest_table['score']
read_table['media'] = ingest_table['media']
read_table['statistics'] = ingest_table['statistics']
read_table['account'] = ingest_table['account']
read_table['languageCode'] = ingest_table['languageCode']
read_table['legacyId'] = ingest_table['legacyId']
read_table['id'] = ingest_table['id']
if 'videoLengthMS' in ingest_table.columns:
    read_table['videoLengthMS'] = ingest_table['videoLengthMS']
else:
    read_table['videoLengthMS'] = ''

if 'imageText' in ingest_table.columns:
    read_table['imageText'] = ingest_table['imageText']
else:
    read_table['imageText'] = ''
read_table['facebook_id'] = ingest_table['facebook_id']

def main_table():

    if os.path.exists('C:/Users/User/Desktop/sph/main.txt'):
        read_table.to_csv('C:/Users/User/Desktop/sph/main.txt', sep='|', index=True, mode='a', header=False)
    else:
        read_table.to_csv('C:/Users/User/Desktop/sph/main.txt', sep='|', index=True, mode='a', header=True)

def media_table():
    media_table = pd.DataFrame(columns=['platformid','type','url','height','width','full'])
    for x in range(0, len(ingest_table)): 

        df = ingest_table['media'][x][0]
        media_table = media_table._append({'platformid':ingest_table['platformId'][x],'type': df['type'], 'url': df['url'], 'height': df['height'], 'width': df['width'], 'full': df['full']}, ignore_index=True)

    if os.path.exists('C:/Users/User/Desktop/sph/media.txt'):
        media_table.to_csv('C:/Users/User/Desktop/sph/media.txt', sep='|', index=True, mode='a', header=False)
    else:
        media_table.to_csv('C:/Users/User/Desktop/sph/media.txt', sep='|', index=True, mode='a', header=True)

def account_table():
    account_table = pd.DataFrame(columns=['captionid','name','handle','profileImage','subscriberCount','url','platform','platformId','accountType','pageAdminTopCountry','verified'])
    for x in range(0, len(ingest_table)): 

        df = ingest_table['account'][x]
        account_table = account_table._append({'captionid': df['id'],'name':df['name'],'handle':df['handle'],'profileImage':df['profileImage'],'subscriberCount':df['subscriberCount'],'url':df['url'],'platform':df['platform'],'platformId':df['platformId'],'accountType':df['accountType'],'pageAdminTopCountry':df['pageAdminTopCountry'],'verified':df['verified']}, ignore_index=True)
        account_table = account_table.drop_duplicates()
    
    if os.path.exists('C:/Users/User/Desktop/sph/account.txt'):
        account_table.to_csv('C:/Users/User/Desktop/sph/account.txt', sep='|', index=True, mode='a', header=False)
    else:
        account_table.to_csv('C:/Users/User/Desktop/sph/account.txt', sep='|', index=True, mode='a', header=True)

def statistic_table():
    statistic_table = pd.DataFrame(columns=['platformid','actual_likeCount','actual_shareCount','actual_commentCount','actual_loveCount','actual_wowCount','actual_hahaCount','actual_sadCount','actual_angryCount','actual_thankfulCount','actual_careCount','expected_likeCount','expected_shareCount','expected_commentCount','expected_loveCount','expected_wowCount','expected_hahaCount','expected_sadCount','expected_angryCount','expected_thankfulCount','expected_careCount'])
    for x in range(0, len(ingest_table)): 
        actual = ingest_table['statistics'][x]['actual']
        expected = ingest_table['statistics'][x]['expected']
        statistic_table = statistic_table._append({'platformid':ingest_table['platformId'][x],'actual_likeCount':actual['likeCount'],'actual_shareCount':actual['shareCount'],'actual_commentCount':actual['commentCount'],'actual_loveCount':actual['loveCount'],'actual_wowCount':actual['wowCount'],'actual_hahaCount':actual['hahaCount'],'actual_sadCount':actual['sadCount'],'actual_angryCount':actual['angryCount'],'actual_thankfulCount':actual['thankfulCount'],'actual_careCount':actual['careCount'],'expected_likeCount':expected['likeCount'],'expected_shareCount':expected['shareCount'],'expected_commentCount':expected['commentCount'],'expected_loveCount':expected['loveCount'],'expected_wowCount':expected['wowCount'],'expected_hahaCount':expected['hahaCount'],'expected_sadCount':expected['sadCount'],'expected_angryCount':expected['angryCount'],'expected_thankfulCount':expected['thankfulCount'],'expected_careCount':expected['careCount']}, ignore_index=True)

    
    if os.path.exists('C:/Users/User/Desktop/sph/statistic.txt'):
        statistic_table.to_csv('C:/Users/User/Desktop/sph/statistic.txt', sep='|', index=True, mode='a', header=False)
    else:
        statistic_table.to_csv('C:/Users/User/Desktop/sph/statistic.txt', sep='|', index=True, mode='a', header=True)

      
main_table()
# media_table()
#account_table()
# statistic_table()