import pandas as pd

#Paramenter & Definations
media_file='C:/Users/K3nXz/Desktop/sph/leaderboard_breakdown.txt'

df = pd.read_csv(media_file, delimiter='|')

df['breakdown_id'] = df.groupby(['id', 'date']).cumcount() + 1


df.to_csv('C:/Users/K3nXz/Desktop/sph/leaderboard_breakdown.txt', sep='|', index=False, header=True)
