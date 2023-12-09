import pandas as pd

#Paramenter & Definations
leaderboard_account_file='C:/Users/K3nXz/Desktop/sph/leaderboard_account.txt'

df = pd.read_csv(leaderboard_account_file, delimiter='|')
print(df['platformId'])
df['pageDescription'] = df['pageDescription'].str.replace('\n', '')
df['platformId'] = df['platformId'].fillna(0).astype('int64')



df.to_csv('C:/Users/K3nXz/Desktop/sph/leaderboard_account.txt', sep='|', index=False, header=True)