import pandas as pd
import os

#Parameter
account='C:/Users/K3nXz/Desktop/sph/account.txt'

df = pd.read_csv(account, delimiter='|')
df_new = pd.read_csv(account_new, delimiter='|')

# df_diff = df.compare(df_new)
# print(df_diff)

df_diff = pd.merge(df, df_new, on='captionid', how='inner', indicator=True)

# select only the rows that are different between the two dataframes

print(df_diff)