import os

from process_data.utils import *
from process_data.get_information import *
from api.tasks import get_sensor_data, get_energy_data, get_activities_data
from api.utils import convert_to_unix_timestamp

bg_dir = os.getcwd() + '/images/chart/'
MISSING_THRESHOLD = 11*60  # seconds
ACTIVITIES_THRESHOLD = 5*60  # seconds


def extract_user_data(user_id, device_id, track_day):

    date = pd.to_datetime(track_day)

    ###############################################################################################################
    '''
        Process data frame sensor
    '''
    df_sensor = get_sensor_data(device_id, user_id, convert_to_unix_timestamp(track_day), 24)

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
    df_sensor, df_sensor_missing = process_missing_data(df_sensor, user_id, device_id, date, 'sensor')

    ###############################################################################################################
    '''
        Process data frame energy
    '''
    df_energy = get_energy_data(device_id, user_id, convert_to_unix_timestamp(track_day), 24)

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
    df_energy, df_energy_missing = process_missing_data(df_energy, user_id, device_id, date, 'energy')

    ###############################################################################################################
    '''
        Process data frame activities
    '''
    cols = ['event_type', 'timestamp', 'power', 'temperature', 'fan_speed', 'operation_mode']

    df_activities = get_activities_data(device_id, user_id, convert_to_unix_timestamp(track_day), 24)

    ## DROP DUPLICATED
    df_activities = drop_duplicated(df_activities)

    ## RESAMPLE
    if len(df_activities) > 0:
        df2 = process_activities(df_activities)
        df2['timestamp'] = pd.to_datetime(df2['timestamp'].apply(lambda x: dt.fromtimestamp(x)))
        df2.sort_values(by='timestamp', inplace=True)
    else:
        df2 = pd.DataFrame(columns=cols)

    df2 = df2[cols].reset_index(drop=True)

    df_act = pd.DataFrame(columns=cols)
    for i in range(len(df2)):
        curr_t = df2['timestamp'].iloc[i]

        df_next = df2[(df2['timestamp'] > curr_t) & (df2['timestamp'] < curr_t + datetime.timedelta(seconds=ACTIVITIES_THRESHOLD))]

        if len(df_next) != 0:
            continue
        else:
            df_act = pd.concat([df_act, pd.DataFrame([df2.iloc[i]], columns=cols)], ignore_index=True)

    df_activities = df_act

    df_missing = pd.concat([df_sensor_missing, df_energy_missing], ignore_index=True)

    return df_sensor, df_energy, df_activities, df_missing