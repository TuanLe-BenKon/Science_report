import numpy as np
import pandas as pd
from scipy import stats
from sqlalchemy.types import TypeEngine

from base_processing_data import get_last_row, extract_sql_data, load_df_data

DEST_TABLE = "transformed_sensor"
SOURCE_TABLE = "sensor_data"


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


def generate_sql_statement(df_last_row: pd.DataFrame) -> str:
    sql_statement = """
        SELECT s.id, device_id, d.address, d.alias, user_id, u.name, temperature, humidity, s.timestamp
        FROM public.{} as s 
            JOIN public.devices as d ON s.device_id = d.id 
            JOIN public.users as u ON d.user_id = u.id
        ORDER BY s.timestamp, device_id, address ASC;
    """.format(SOURCE_TABLE)
    if not df_last_row.empty:
        sql_statement = """
            SELECT s.id, device_id, d.address, d.alias, user_id, u.name, temperature, humidity, s.timestamp
            FROM public.{} as s 
                JOIN public.devices as d ON s.device_id = d.id 
                JOIN public.users as u ON d.user_id = u.id
            WHERE s.timestamp > '{}'
            ORDER BY s.timestamp, device_id, address ASC;
        """.format(SOURCE_TABLE, df_last_row["timestamp"][0])
    
    return sql_statement


def sensor_ETL(conn_engine: TypeEngine, des_engine: TypeEngine) -> bool:
    last_row = get_last_row(DEST_TABLE, des_engine)
    sensor_sql = generate_sql_statement(last_row)

    df_sensor_raw = extract_sql_data(conn_engine, sensor_sql)
    if df_sensor_raw.empty:
        print("DataFrame has no data value")
        return

    df_proccessed_sensor = process_sensor_data(df_sensor_raw)
    df_proccessed_sensor = df_proccessed_sensor.assign(device_id = df_sensor_raw["device_id"],
                                            mac_address=df_sensor_raw["address"],
                                            alias=df_sensor_raw["alias"],
                                            user_id=df_sensor_raw["user_id"],
                                            name=df_sensor_raw["name"])

    load_df_data(df_proccessed_sensor, des_engine, DEST_TABLE)
    return True