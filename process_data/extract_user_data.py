import os
import time
import datetime
import numpy as np

from process_data.utils import *
from api.tasks import *

MISSING_THRESHOLD = 11*60  # seconds
ACTIVITIES_THRESHOLD = 5*60  # seconds


def convert_to_unix_timestamp(_time: str) -> int:
    __time = pd.to_datetime(_time)
    return int(time.mktime(__time.timetuple()))


def extract_user_data(user_id, device_id: str, track_day: str):

    date = pd.to_datetime(track_day)

    ###############################################################################################################
    '''
        Process data frame sensor
    '''
    t_sensor = time.time()
    df_sensor = get_sensor_data(device_id, user_id, convert_to_unix_timestamp(track_day), 24)
    t_sensor = time.time() - t_sensor

    ## Change UTC Time to Timestamp and Sort dataframe
    df_sensor = df_sensor[['timestamp', 'temperature', 'humidity']]
    df_sensor['timestamp'] = pd.to_datetime(df_sensor['timestamp'].apply(lambda x: dt.fromtimestamp(x)))
    df_sensor.sort_values(by='timestamp', inplace=True)
    df_sensor.reset_index(drop=True, inplace=True)

    ## DROP DUPLICATED
    df_sensor = drop_duplicated(df_sensor)

    ## OUTLIERS
    df_sensor = process_outliers(df_sensor)

    ## MISSING DATA
    df_sensor = process_missing_data(df_sensor, 'sensor')

    ###############################################################################################################
    '''
        Process data frame energy
    '''
    t_energy = time.time()
    if not get_accessories_df(device_id).empty:
        df_energy = get_energy_data_accessory(device_id, convert_to_unix_timestamp(track_day), 24)
    else:
        df_energy = get_energy_data(device_id, convert_to_unix_timestamp(track_day), 24)
    t_energy = time.time() - t_energy

    ## Change UTC Time to Timestamp and Sort dataframe
    df_energy = df_energy[['timestamp', 'power', 'energy']]
    df_energy['timestamp'] = pd.to_datetime(df_energy['timestamp'].apply(lambda x: dt.fromtimestamp(x)))
    df_energy.sort_values(by='timestamp', inplace=True)
    df_energy.reset_index(drop=True, inplace=True)

    ## DROP DUPLICATED
    df_energy = drop_duplicated(df_energy)

    ## RESET ENERGY
    df_energy = process_reset(df_energy)

    ## MISSING DATA
    df_energy = process_missing_data(df_energy, 'energy')

    ###############################################################################################################
    '''
        Process data frame activities
    '''
    cols = ['event_type', 'timestamp', 'power', 'temperature', 'fan_speed', 'operation_mode']

    t_act = time.time()
    df_activities = get_activities_data(device_id, user_id, convert_to_unix_timestamp(track_day), 24)
    t_act = time.time() - t_act

    print('Total query time of device ', user_id, ' = ', t_sensor + t_energy + t_act)

    ## RESAMPLE
    if len(df_activities) > 0:
        df2 = process_activities(df_activities)

        ## DROP DUPLICATED
        df2 = drop_duplicated(df2)

        df2['timestamp'] = pd.to_datetime(df2['timestamp'].apply(lambda x: dt.fromtimestamp(x)))
        df2.sort_values(by='timestamp', inplace=True)
    else:
        df2 = pd.DataFrame(columns=cols)

    df2 = df2[cols].reset_index(drop=True)

    df_act = pd.DataFrame(columns=cols)
    for i in range(len(df2)):
        if df2['operation_mode'].iloc[i] is None:
            continue

        if 'scheduler' in df2['event_type'].iloc[i]:
            df_act = pd.concat([df_act, pd.DataFrame([df2.iloc[i]], columns=cols)], ignore_index=True)
        else:
            curr_t = df2['timestamp'].iloc[i]

            df_next = df2[(df2['timestamp'] > curr_t) & (df2['timestamp'] < curr_t + datetime.timedelta(seconds=ACTIVITIES_THRESHOLD))]

            if len(df_next) != 0:
                continue
            else:
                df_act = pd.concat([df_act, pd.DataFrame([df2.iloc[i]], columns=cols)], ignore_index=True)

    df_activities = df_act

    return df_sensor, df_energy, df_activities


# Get preprocess energy data
def extract_energy_data(device_id: str, track_day: str):
    df_energy = get_energy_data(device_id, convert_to_unix_timestamp(track_day), 24)

    ## Change UTC Time to Timestamp and Sort dataframe
    df_energy = df_energy[['timestamp', 'power', 'energy']]
    df_energy['timestamp'] = pd.to_datetime(df_energy['timestamp'].apply(lambda x: dt.fromtimestamp(x)))
    df_energy.sort_values(by='timestamp', inplace=True)
    df_energy.reset_index(drop=True, inplace=True)

    ## DROP DUPLICATED
    df_energy = drop_duplicated(df_energy)

    ## RESET ENERGY
    df_energy = process_reset(df_energy)

    ## MISSING DATA
    df_energy = process_missing_data(df_energy, 'energy')

    return df_energy
