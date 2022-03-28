import os
import requests
import datetime
from datetime import datetime as dt
import pandas as pd
import json
import numpy as np
from process_data.get_information import *

MISSING_THRESHOLD = 11*60 # seconds

# Activities parser
OPS_MODES = {
    1: "Dry",
    2: "Fan Only",
    3: "Heat",
    4: "Cool",
    7: "Auto"
}


def parse_payload_str(row):
    # payload = row["payload"].replace("=>", ": ")
    # payload_dict = json.loads(payload)
    payload_dict = row.get("payload", {})

    power = payload_dict.get("power")
    temp = payload_dict.get("temperature")
    fan_speed = payload_dict.get("fan_speed")
    ops_mode = OPS_MODES.get(payload_dict.get("operation_mode"))
    return pd.Series([power, temp, fan_speed, ops_mode])


def parse_date(row):
    date = dt.utcfromtimestamp(row['timestamp'])
    return date.strftime('%Y-%m-%d %H:%M:%S')


def process_activities(df_activities: pd.DataFrame) -> pd.DataFrame:
    df_activities[["power", "temperature", "fan_speed", "operation_mode"]] = df_activities.apply(parse_payload_str, axis=1)
    df_activities["date"] = df_activities.apply(parse_date, axis=1)

    necessary_columns = ["device_id", "event_type", "is_success", "timestamp", "date", "power", "temperature", "fan_speed", "operation_mode"]
    return df_activities.loc[df_activities["is_success"] == True][necessary_columns]


## Drop duplicated data
def drop_duplicated(df, f='n'):

    if f != 'n':
        sensor_data_duplicated = df.duplicated(subset='timestamp')
        sensor_data_duplicated_list = np.where(sensor_data_duplicated == True)[0]

        f.write('Duplicated list: {:d} elements\n'.format(len(sensor_data_duplicated_list)))
        for i in range(len(sensor_data_duplicated_list)):
            f.write(str(df['timestamp'].iloc[sensor_data_duplicated_list[i]]) + '\n')
        f.write('------------------------------------------\n')

        df.drop_duplicates(inplace=True)
        df.reset_index(drop=True, inplace=True)

    return df


## Outliers
def process_outliers(df):

    Q1 = df['temperature'].quantile(0.2)
    Q3 = df['temperature'].quantile(0.8)

    IQR = Q3 - Q1

    outliers_filter = ((df['temperature'] < (Q1 - 1.5 * IQR)) | (df['temperature'] > (Q3 + 1.5 * IQR)))

    outliers = df.loc[outliers_filter].index

    for i in range(len(outliers)):
        df.at[outliers[i], 'temperature'] = df['temperature'].iloc[outliers[i] - 1] 
        df.at[outliers[i], 'humidity'] = df['humidity'].iloc[outliers[i] - 1]

    return df


## Reset Energy
def process_reset(df):
    if len(df) != 0:
        # min_energy = df['energy'].min()
        # max_energy = df['energy'].max()
        # start_energy = df['energy'].iloc[0]

        # reset = np.where((df['energy'] < min_energy) | (df['energy'] - min_energy > 100000))[0]

        # for i in range(len(reset)):
        #     df.at[reset[i], 'energy'] = df['energy'].iloc[reset[i] - 1] 
        #     df.at[reset[i], 'power'] = df['power'].iloc[reset[i] - 1]

        Q1 = df['energy'].quantile(0.2)
        Q3 = df['energy'].quantile(0.8)

        IQR = Q3 - Q1

        outliers_filter = ((df['energy'] < (Q1 - 1.5 * IQR)) | (df['energy'] > (Q3 + 1.5 * IQR)))

        outliers = df.loc[outliers_filter].index

        for i in range(len(outliers)):

            if outliers[i] == 0:
                df.at[outliers[i], 'energy'] = df['energy'].iloc[outliers[i] + 1] 
                df.at[outliers[i], 'power'] = df['power'].iloc[outliers[i] + 1]
            else:
                df.at[outliers[i], 'energy'] = df['energy'].iloc[outliers[i] - 1] 
                df.at[outliers[i], 'power'] = df['power'].iloc[outliers[i] - 1]

    return df


## Missing data
def process_missing_data(df, user_id, device_id, date, data_type, f='n'):

    missing_list = []
    missing_time = 0
    cnt = 0
    df_missing = pd.DataFrame()
    cols = ['username', 'device_name', 'total missing time', 'type']

    if len(df) == 0:
        df_missing = pd.concat([df_missing,
                                pd.DataFrame([[username[user_id], 
                                            device_name[device_id], 
                                            -1, 
                                            data_type]], 
                                            columns=cols)
                                ], 
                                ignore_index=True)
    else:
        first_range = pd.to_timedelta((df['timestamp'].iloc[0] - date).total_seconds(), 'S').total_seconds()
        if first_range > MISSING_THRESHOLD:
            cnt += 1
            missing_time += first_range
            if f != 'n':
                f.write(str(date) + ' - ' + str(df['timestamp'].iloc[0]) + ' - Delta time: ' + str(first_range) + '\n')

        for i in range(len(df) - 1):
            curr_time = df['timestamp'].iloc[i]
            next_time = df['timestamp'].iloc[i + 1]
            delta_time = next_time - curr_time

            if delta_time.total_seconds() > MISSING_THRESHOLD:
                missing_list.append((i, i + 1, curr_time, next_time, delta_time))
                missing_time += delta_time.total_seconds()

        last_range = (date + datetime.timedelta(days=1) - df['timestamp'].iloc[-1]).total_seconds()
        if last_range > MISSING_THRESHOLD:
            cnt += 1
            missing_time += last_range
            if f != 'n':
                f.write(str(date + datetime.timedelta(days=1)) + ' - ' + str(df['timestamp'].tail(1).iloc[0]) + ' - Delta time: ' + str(last_range) + '\n')

        df_missing = pd.concat([df_missing,
                                pd.DataFrame([[username[user_id], 
                                            device_name[device_id], 
                                            missing_time, 
                                            data_type]], 
                                            columns=cols)
                                ], 
                                ignore_index=True)

    if f != 'n':    
        f.write('\n')
        f.write('Missing data list: {:d} ranges\n'.format(len(missing_list) + cnt))
        for i in range(len(missing_list)):
            f.write(str(missing_list[i][2]) + ' - ' + str(missing_list[i][3]) + ' - Delta time: ' + str(missing_list[i][4]) + '\n')


    ## Resample missing data
    k = 0
    new_list = []

    for i in range(len(missing_list)):
        info = missing_list[i]

        df_temp = pd.DataFrame(columns=df.columns)

        # Get time
        df1 = df[k:info[0]]

        df_temp['timestamp'] = pd.date_range(start=info[2], end=info[3], freq='30S')

        if info[4].total_seconds() > MISSING_THRESHOLD:
            if data_type == 'sensor':
                df_temp['temperature'] = np.nan
                df_temp['humidity'] = np.nan
            else:
                df_temp['energy'] = np.nan
                df_temp['power'] = np.nan
        else:
            if data_type == 'sensor':
                df_temp['temperature'] = (df['temperature'].iloc[info[0]] + df['temperature'].iloc[info[1]]) / 2
                df_temp['humidity'] = (df['humidity'].iloc[info[0]] + df['humidity'].iloc[info[1]]) / 2
            else:
                df_temp['energy'] = (df['energy'].iloc[info[0]] + df['energy'].iloc[info[1]]) / 2
                df_temp['power'] = (df['power'].iloc[info[0]] + df['power'].iloc[info[1]]) / 2

        df1 = pd.concat([df1, df_temp], ignore_index=True)

        new_list.append(df1)
        k = info[1] + 1

    df1 = df[k:len(df)]
    new_list.append(df1)

    new_df = pd.concat(new_list, ignore_index=True)
    new_df.drop_duplicates(inplace=True)

    return new_df, df_missing


## Get Energy Consumption
def get_energy_consumption(df):
    return df['energy'].max() - df['energy'].min()


## Get Working Time
def get_working_time(df):
    
    df_working = df[df['power'] >= 8].reset_index(drop=True)
    working_time = 0
    
    for i in range(len(df_working) - 1):
        if (df_working['timestamp'].iloc[i + 1] - df_working['timestamp'].iloc[i]).total_seconds() > MISSING_THRESHOLD:
            pass
        else:
            working_time += (df_working['timestamp'].iloc[i + 1] - df_working['timestamp'].iloc[i]).total_seconds()

    return working_time


## Cut working time

def calc_energy_saving(df, track_day, hour_start=8, hour_end=21):

    if len(df) == 0:
        return 0

    date = pd.to_datetime(track_day)
    start_time = date + pd.to_timedelta(hour_start, 'H')
    end_time = date + pd.to_timedelta(hour_end, 'H')

    df = df[(df['timestamp'] >= start_time) & (df['timestamp'] <= end_time)]
    
    max_e = df['energy'].max()
    min_e = df['energy'].min()

    if np.isnan(max_e) or np.isnan(min_e):
        return 0
    else:
        return max_e - min_e

def extract_energy_df(bg_dir, user_id, device_id, track_day):
    '''
        Process data frame energy
    '''
    date = pd.to_datetime(track_day)

    df_energy = pd.read_csv('{}/{}/{}/{}/energy_data-{}.csv'.format(
        bg_dir,
        username[user_id],
        device_name[device_id],
        track_day,
        track_day
    ))

    ## Change UTC Time to Timestamp and Sort dataframe
    df_energy = df_energy[['timestamp', 'power', 'energy']]
    df_energy['timestamp'] = pd.to_datetime(df_energy['timestamp'].apply(lambda x: dt.fromtimestamp(x) + datetime.timedelta(hours=7)))
    df_energy.sort_values(by='timestamp', inplace=True)
    df_energy.reset_index(drop=True, inplace=True)

    ## DROP DUPLICATED
    df_energy = drop_duplicated(df_energy)

    ## RESET ENERGY
    df_energy = process_reset(df_energy)

    ## MISSING DATA
    df_energy, df_energy_missing = process_missing_data(df_energy, user_id, device_id, date, 'energy')

    return df_energy