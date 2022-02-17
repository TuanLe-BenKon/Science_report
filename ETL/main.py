import pandas as pd
from dotenv import load_dotenv

from base_processing_data import load_df_data, extract_sql_data
from processing_energy_data import energy_extract_transform
from processing_sensor_data import sensor_extract_transform
from processing_activities import activities_extract_transform

load_dotenv()


if __name__ == "__main__":
    conn_url = "postgresql+psycopg2://postgres:admin123@localhost:5432/b002"
    des_url = (
        "postgresql+psycopg2://postgres:admin123@localhost:5432/benkon_transformed_data"
    )

    ### Getting device_id to data frame
    sql = """
        SELECT  d.id as device_id, d.address, d.alias, user_id, u.name
        FROM  public.devices as d 
            JOIN public.users as u ON d.user_id = u.id"""
    df_raw_data = extract_sql_data(conn_url, sql)

    df_energy_ret = pd.DataFrame()
    df_sensor_ret = pd.DataFrame()
    df_activites_ret = pd.DataFrame()
    for index, row in df_raw_data.iterrows():
        ## Process enegery data
        df_processed_energy = energy_extract_transform(conn_url, row)
        df_energy_ret = pd.concat(
            [df_sensor_ret, df_processed_energy], ignore_index=True
        )

        ## Process sensor data
        df_processed_sensor = sensor_extract_transform(conn_url, row)
        df_sensor_ret = pd.concat(
            [df_sensor_ret, df_processed_sensor], ignore_index=True
        )

        ## Process activities data
        df_activities_sensor = activities_extract_transform(conn_url, row)
        df_activities_ret = pd.concat(
            [df_activites_ret, df_activities_sensor], ignore_index=True
        )

    print("Energy data info")
    print(df_energy_ret.info())
    print("Sensor data info")
    print(df_sensor_ret.info())
    print("Activities data info")
    print(df_activities_ret.info())

    # Persist data to the database
    load_df_data(df_energy_ret, des_url, "transformed_energy")
    load_df_data(df_sensor_ret, des_url, "transformed_sensor")
    load_df_data(df_activities_ret, des_url, "transformed_activites")
