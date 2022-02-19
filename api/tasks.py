import os
from uuid import UUID

import pandas as pd
from flask import Response

from sqlalchemy import create_engine

THRESHOLD = 1000
URL = os.environ.get("SOURCE_DATABASE_URL")


def get_device_data(device_id: UUID, user_id: int, init_timestamp: int) -> Response:
    engine = create_engine(URL)
    sql_statement = """
        SELECT e.id, device_id, d.address, d.alias, user_id, u.name, power, energy, e.timestamp
        FROM public.energy_data as e 
            JOIN public.devices as d ON e.device_id = d.id 
            JOIN public.users as u ON d.user_id = u.id
        WHERE device_id='{}' 
            AND user_id='{}'
            AND e.timestamp>={}
        ORDER BY e.timestamp, device_id, address ASC;
    """.format(
        device_id, user_id, init_timestamp
    )
    try:
        df = pd.read_sql(sql_statement, con=engine)
    except:
        df = pd.DataFrame()

    return df


def exceed_threshold(
    start_timestamp: int, last_timestamp: int, current_power: int
) -> bool:
    if last_timestamp >= start_timestamp + 60 * 60 * 2:
        if current_power > THRESHOLD:
            print(current_power)
            return True

    return False


def energy_alert(device_id: UUID, user_id: int, init_timestamp: int):
    df = get_device_data(device_id, user_id, init_timestamp)
    if df.empty:
        return "No data value"
    start_timestamp = df.iloc[0]["timestamp"]
    last_timestamp = df.tail(1)["timestamp"].values[0]
    power = df.tail(1)["power"].values[0]
    result = exceed_threshold(start_timestamp, last_timestamp, power)
    if result:
        return "exceed power asumption"

    return "working normal"
