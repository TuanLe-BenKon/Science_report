import pandas as pd
import os

df_info = pd.read_csv(os.getcwd() + '/device_info.csv')

# Convert user_id and user_name to dict type
df_username = df_info[['user_id', 'user_name']]
df_username = df_username.drop_duplicates().reset_index(drop=True)
username = dict(zip(df_username['user_id'], df_username['user_name']))

# Convert device_id and device_name to dict type
df_device_name = df_info[['device_id', 'device_name']]
df_device_name = df_device_name.drop_duplicates().reset_index(drop=True)
device_name = dict(zip(df_device_name['device_id'], df_device_name['device_name']))
