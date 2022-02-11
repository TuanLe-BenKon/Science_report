import json
from datetime import datetime

import pandas as pd

from base_processing_data import extract_sql_data


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

    neccessary_columns = ["device_id", "event_type", "is_success", "timestamp", "date", "temperature", "fan_speed", "operation_mode"]
    return df_activities.loc[df_activities["is_success"] == True][neccessary_columns]


def activities_extract_transform(conn_url: str, info: pd.Series) -> pd.DataFrame:
    sql_get_activities = """
        SELECT da.id, device_id, d.address, d.alias, user_id, u.name, da.timestamp, extra, event_type, payload, is_success 
        FROM public.device_activities as da 
            JOIN public.devices as d ON da.device_id = d.id
            JOIN public.users as u ON d.user_id = u.id
        WHERE device_id = '{}'
        ORDER BY da.timestamp, device_id, address ASC;
    """.format(info["device_id"])

    df_activities_raw = extract_sql_data(conn_url, sql_get_activities) 
    if df_activities_raw.empty:
        print("DataFrame has no data value")
        return

    df_proccesed_activities = process_activites(df_activities_raw)
    df_proccesed_activities = df_proccesed_activities.assign(device_id = info["device_id"],
                                                mac_address=info["address"],
                                                alias=info["alias"],
                                                user_id=info["user_id"],
                                                name=info["name"])

    return df_proccesed_activities 