import numpy as np
import pandas as pd
from scipy import stats
from sqlalchemy.types import TypeEngine

from base_processing_data import extract_sql_data


def process_engery_data(df: pd.DataFrame) -> pd.DataFrame:
    ## remove duplications
    df_result = df.groupby("timestamp")["energy", "power"].mean().reset_index()

    return df_result


def process_sensor_data(df: pd.DataFrame) -> pd.DataFrame:
    df_sensor = df.groupby("timestamp")["temperature", "humidity"].mean().reset_index()
    # Finding z-score of each point
    z_temp = abs(stats.zscore(df_sensor['temperature']))
    z_humid = abs(stats.zscore(df_sensor['humidity']))

    a_std = np.std(df_sensor['temperature'])
    a = df_sensor.iloc[np.where(z_temp > 2)]
    index_list = a.index
    for i in index_list:
        # Replace with previous value
        df_sensor['temperature'].iloc[i] = df_sensor['temperature'].iloc[i-1]
        df_sensor['humidity'].iloc[i] = df_sensor['humidity'].iloc[i-1]

    return df_sensor
    

def sensor_extract_transform(conn_url: str, info: pd.Series) -> pd.DataFrame:
    ## Extract raw data
    sql_get_sensor = """
        SELECT s.id, device_id, d.address, d.alias, user_id, u.name, temperature, humidity, s.timestamp
        FROM public.sensor_data as s 
            JOIN public.devices as d ON s.device_id = d.id 
            JOIN public.users as u ON d.user_id = u.id
        WHERE device_id='{}'
        ORDER BY s.timestamp, device_id, address ASC;
        """.format(info["device_id"])

    df_sensor_raw = extract_sql_data(conn_url, sql_get_sensor)
    if df_sensor_raw.empty:
        print("DataFrame has no data value")
        return
    ## Transform the data
    df_processed_sensor = process_sensor_data(df_sensor_raw)
    df_processed_sensor = df_processed_sensor.assign(device_id = info["device_id"],
                                                mac_address=info["address"],
                                                alias=info["alias"],
                                                user_id=info["user_id"],
                                                name=info["name"])
    return df_processed_sensor


# if __name__=="__main__":
#     conn_url = 'postgresql+psycopg2://postgres:admin123@localhost:5432/b002'
#     des_url = 'postgresql+psycopg2://postgres:admin123@localhost:5432/benkon_transformed_data'
#     conn_engine = create_engine(conn_url)
#     des_engine = create_engine(des_url)

#     ### Getting device_id to data frame
#     sql = """
#         SELECT  d.id as device_id, d.address, d.alias, user_id, u.name
#         FROM  public.devices as d 
#             JOIN public.users as u ON d.user_id = u.id"""
#     df = load_sql_data(conn_engine, sql)
#     df_result = pd.DataFrame()
#     for index, row in df.iterrows():
#         ## Process sensor data
#         df_sensor = sensor_pipeline(conn_engine, des_engine, row)
#         df_result = pd.concat([df_result, df_sensor], ignore_index=True)

#     load_df_data(df_result, des_engine, "transformed_sensor")