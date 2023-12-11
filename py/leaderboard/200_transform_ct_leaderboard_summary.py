import pandas as pd

#Paramenter & Definations
media_file='/home/kenny/txt/leaderboard_summary.txt'

df = pd.read_csv(media_file, delimiter='|')
df.fillna(0, inplace=True)
df['loveCount'] = df['loveCount'].fillna(0).astype('int')
df['totalInteractionCount'] = df['totalInteractionCount'].fillna(0).astype('int')
df['wowCount'] = df['wowCount'].fillna(0).astype('int')
df['thankfulCount'] = df['thankfulCount'].fillna(0).astype('int')
df['interactionRate'] = df['interactionRate'].fillna(0).astype('int')
df['likeCount'] = df['likeCount'].fillna(0).astype('int')
df['hahaCount'] = df['hahaCount'].fillna(0).astype('int')
df['commentCount'] = df['commentCount'].fillna(0).astype('int')
df['shareCount'] = df['shareCount'].fillna(0).astype('int')
df['careCount'] = df['careCount'].fillna(0).astype('int')
df['sadCount'] = df['sadCount'].fillna(0).astype('int')
df['angryCount'] = df['angryCount'].fillna(0).astype('int')
df['totalVideoTimeMS'] = df['totalVideoTimeMS'].fillna(0).astype('int64')
df['postCount'] = df['postCount'].fillna(0).astype('int')



# df['breakdown_id'] = df.groupby(['id', 'date']).cumcount() + 1


df.to_csv('/home/kenny/txt/leaderboard_summary.txt', sep='|', index=False, header=True)
