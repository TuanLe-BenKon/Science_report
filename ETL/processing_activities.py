from datetime import datetime

import pandas as pd
from sqlalchemy.types import TypeEngine

from base_processing_data import get_last_row, extract_sql_data, load_df_data


OPS_MODES = {
    1: "Dry",
    2: "Fan Only",
    3: "Heat",
    4: "Cool",
    7: "Auto"
}

SOURCE_TABLE = "device_activities"
DEST_TABLE = "transformed_activites"



def parse_payload_str(row):
    # payload = row["payload"].replace("=>", ": ")
    # payload_dict = json.loads(payload)
    payload_dict = row.get("payload", {})
    
    temp = payload_dict.get("temperature")
    fan_speed = payload_dict.get("fan_speed")
    ops_mode = OPS_MODES.get(payload_dict.get("operation_mode"))
    return pd.Series([temp, fan_speed, ops_mode])


def parse_date(row):
    date = datetime.utcfromtimestamp(row['timestamp'])
    return date.strftime('%Y-%m-%d %H:%M:%S')


def process_activites(df_activities: pd.DataFrame) -> pd.DataFrame:
    df_activities[["temperature", "fan_speed", "operation_mode"]] = df_activities.apply(parse_payload_str, axis=1)
    df_activities["date"] = df_activities.apply(parse_date, axis=1)

    neccessary_columns = ["device_id", "event_type", "user_id", "address", "is_success", "timestamp", "date", "temperature", "fan_speed", "operation_mode"]
    return df_activities.loc[df_activities["is_success"] == True][neccessary_columns]


def generate_sql_statement(df_last_row: pd.DataFrame) -> str:
    sql_statement = """
        SELECT da.id, device_id, d.address, d.alias, user_id, u.name, da.timestamp, extra, event_type, payload, is_success 
        FROM public.{} as da 
            JOIN public.devices as d ON da.device_id = d.id
            JOIN public.users as u ON d.user_id = u.id
        ORDER BY da.timestamp, device_id, address ASC;
    """.format(SOURCE_TABLE)
    if not df_last_row.empty:
        sql_statement = """
            SELECT da.id, device_id, d.address, d.alias, user_id, u.name, da.timestamp, extra, event_type, payload, is_success 
            FROM public.{} as da 
                JOIN public.devices as d ON da.device_id = d.id
                JOIN public.users as u ON d.user_id = u.id
            WHERE da.timestamp > '{}'
            ORDER BY da.timestamp, device_id, address ASC;
        """.format(SOURCE_TABLE, df_last_row["timestamp"][0])

    return sql_statement


def activities_ETL(conn_engine: TypeEngine, des_engine: TypeEngine) -> bool:
    last_row = get_last_row(DEST_TABLE, des_engine)
    activities_sql = generate_sql_statement(last_row)

    df_activities_raw = extract_sql_data(conn_engine, activities_sql) 
    if df_activities_raw.empty:
        print("DataFrame has no data value")
        return

    df_proccesed_activities = process_activites(df_activities_raw)
    load_df_data(df_proccesed_activities, des_engine, DEST_TABLE)

    return True