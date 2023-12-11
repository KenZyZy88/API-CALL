import pandas as pd

#Paramenter & Definations
leaderboard_account_file='/home/kenny/txt/leaderboard_account.txt'

df = pd.read_csv(leaderboard_account_file, delimiter='|')
df['pageDescription'] = df['pageDescription'].str.replace('\n', '')
df['platformId'] = df['platformId'].fillna(0).astype('int64')



df.to_csv('/home/kenny/txt/leaderboard_account.txt', sep='|', index=False, header=True)