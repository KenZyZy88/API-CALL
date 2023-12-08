import pandas as pd

#Paramenter & Definations
media_file='/home/kenny/txt/media.txt'

df = pd.read_csv(media_file, delimiter='|')
df['media_id'] = df.groupby(['captionid', 'post_id']).cumcount() + 1


df.to_csv('/home/kenny/txt/media.txt', sep='|', index=False, header=True)
