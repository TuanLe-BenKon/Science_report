import os
import pandas as pd
import gspread

filename = os.getcwd() + '\\key.json'
sa = gspread.service_account(filename=filename)

## Open device_info spreadsheets
sh = sa.open('device_info')

sheet = sh.worksheet('device_info')
rows = sheet.get_all_values()

df_info = pd.DataFrame.from_records(rows[1:], columns=rows[0])

# Convert user_id and user_name to dict type
df_username = df_info[['user_id', 'user_name']]
df_username = df_username.drop_duplicates().reset_index(drop=True)
username = dict(zip(df_username['user_id'], df_username['user_name']))

# Convert device_id and device_name to dict type
df_device_name = df_info[df_info['status'] == '1']
df_device_name = df_device_name[['device_id', 'device_name']]
df_device_name = df_device_name.drop_duplicates().reset_index(drop=True)
device_name = dict(zip(df_device_name['device_id'], df_device_name['device_name']))

## Open device_info spreadsheets
sh = sa.open('Email List by User')
sheet = sh.worksheet('email_list')
rows = sheet.get_all_values()

df_email = pd.DataFrame.from_records(rows[1:], columns=rows[0])