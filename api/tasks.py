import os
import json
import requests
import datetime
from uuid import UUID
from typing import Dict
from requests.structures import CaseInsensitiveDict

import pandas as pd
from flask import Response
from google.cloud import tasks_v2
from sqlalchemy import create_engine
from google.protobuf import timestamp_pb2

THRESHOLD_BY_POWER = {"1.0": 300, "1.5": 500, "2.0": 2000, "2.5": 4000}
IN_SECONDS = 7200  # 2 hours in seconds


def get_energy_data_accessory(device_id: UUID, init_timestamp: int, duration: int) -> pd.DataFrame:
    DATABASE_URL = os.environ.get("SOURCE_DATABASE_URL")
    engine = create_engine(DATABASE_URL)

    bounded_timestamp = init_timestamp + 60 * 60 * duration
    sql_statement = f"""
            SELECT e.power, e.energy, e.timestamp
            FROM public.accessories as a 
                JOIN public.energy_data as e ON a.id = e.accessory_id
            WHERE 
                a.device_id = '{device_id}'
                AND {init_timestamp} <= e.timestamp 
                AND e.timestamp <= {bounded_timestamp}
        """
    try:
        df = pd.read_sql(sql_statement, con=engine)
    except:
        df = pd.DataFrame()
    return df


def get_energy_data(device_id: UUID, user_id: int, init_timestamp: int, duration: int) -> pd.DataFrame:
    DATABASE_URL = os.environ.get("SOURCE_DATABASE_URL")
    engine = create_engine(DATABASE_URL)

    bounded_timestamp = init_timestamp + 60 * 60 * duration
    sql_statement = """
        SELECT e.id, device_id, d.address, btu, d.alias, user_id, u.name, power, energy, e.timestamp
        FROM public.energy_data as e 
            JOIN public.devices as d ON e.device_id = d.id 
            JOIN public.users as u ON d.user_id = u.id
        WHERE device_id='{}' 
            AND user_id='{}'
            AND {} <= e.timestamp 
            AND e.timestamp <= {}
        ORDER BY e.timestamp, device_id, address ASC;
    """.format(
        device_id, user_id, init_timestamp, bounded_timestamp
    )
    try:
        df = pd.read_sql(sql_statement, con=engine)
    except:
        df = pd.DataFrame()
    return df


def get_energy_data_test(device_id: UUID, init_timestamp: int, duration: int) -> pd.DataFrame:
    DATABASE_URL = os.environ.get("SOURCE_DATABASE_URL")
    engine = create_engine(DATABASE_URL)

    bounded_timestamp = init_timestamp + 60 * 60 * duration
    sql_statement = """
                SELECT e.device_id, e.power, e.energy, e.timestamp
                FROM public.energy_data as e 
                WHERE e.device_id='{}' 
                    AND {} <= e.timestamp 
                    AND e.timestamp <= {}
                ORDER BY e.timestamp;
            """.format(
        device_id, init_timestamp, bounded_timestamp
    )
    try:
        df = pd.read_sql(sql_statement, con=engine)
    except:
        df = pd.DataFrame()
    return df


def get_sensor_data(device_id: UUID, user_id: int, init_timestamp: int, duration: int) -> pd.DataFrame:
    DATABASE_URL = os.environ.get("SOURCE_DATABASE_URL")
    engine = create_engine(DATABASE_URL)

    bounded_timestamp = init_timestamp + 60 * 60 * duration
    sql_statement = """
        SELECT s.id, device_id, d.address, d.alias, user_id, u.name, temperature, humidity, s.timestamp
        FROM public.sensor_data as s 
            JOIN public.devices as d ON s.device_id = d.id 
            JOIN public.users as u ON d.user_id = u.id
        WHERE device_id='{}'
            AND {} <= s.timestamp 
            AND s.timestamp <= {}
        ORDER BY s.timestamp, device_id, address ASC;
        """.format(
        device_id, init_timestamp, bounded_timestamp
    )
    try:
        df = pd.read_sql(sql_statement, con=engine)
    except:
        df = pd.DataFrame()

    return df


def get_activities_data(device_id: UUID, user_id: int, init_timestamp: int, duration: int) -> pd.DataFrame:
    DATABASE_URL = os.environ.get("SOURCE_DATABASE_URL")
    engine = create_engine(DATABASE_URL)

    bounded_timestamp = init_timestamp + 60 * 60 * duration
    sql_statement = """
        SELECT da.id, device_id, d.address, d.alias, user_id, u.name, da.timestamp, extra, event_type, payload, is_success 
        FROM public.device_activities as da 
            JOIN public.devices as d ON da.device_id = d.id
            JOIN public.users as u ON d.user_id = u.id
        WHERE device_id = '{}'
            AND {} <= da.timestamp 
            AND da.timestamp <= {}
        ORDER BY da.timestamp, device_id, address ASC;
        """.format(
        device_id, init_timestamp, bounded_timestamp
    )
    try:
        df = pd.read_sql(sql_statement, con=engine)
    except:
        df = pd.DataFrame()

    return df


def horse_power_group(btu: int) -> str:
    if 8500 <= btu and btu <= 9500:
        return "1.0"
    elif 11500 <= btu and btu <= 12500:
        return "1.5"
    elif 17500 <= btu and btu <= 18500:
        return "2.0"
    elif 20500 <= btu and btu <= 22500:
        return "2.5"


def exceed_threshold(
    start_timestamp: int, last_timestamp: int, current_power: int, btu: int
) -> bool:
    horse_power = horse_power_group(btu)

    if last_timestamp >= start_timestamp + 60 * 60 * 1.8:
        if current_power > THRESHOLD_BY_POWER.get(horse_power, 1000):
            return True

    return False


def energy_alert(data: Dict[str, str]) -> int:
    user_id = data.get("user_id")
    device_id = data.get("device_id")
    init_timestamp = data.get("init_timestamp")
    df = get_energy_data(device_id, user_id, init_timestamp)
    if df.empty:
        return 202
    start_timestamp = df.iloc[0]["timestamp"]
    last_timestamp = df.tail(1)["timestamp"].values[0]
    power = df["power"].mean()
    btu = df.tail(1)["btu"].values[0]
    alias = df.tail(1)["alias"].values[0]
    is_exceed = exceed_threshold(start_timestamp, last_timestamp, power, btu)

    if is_exceed:
        # if the energy is higher than threshold send notification to customer
        msg = (
            "[ENERGY NOTIFICATION] Your {} has been running at high power ({} Watt) for 2 hours. \n"
            "You can increase the temperature by 2 degrees to save energy.".format(
                alias, int(power)
            )
        )

        notify_data = {
            "user_id": user_id,
            "title": "Benkon Energy Alert",
            "type": "energy_alert",
            "body": msg,
        }

        headers = CaseInsensitiveDict()
        headers["Content-Type"] = "application/json"
        headers["Authorization"] = "Bearer " + os.environ.get("NOTIFICATION_TOKEN")
        notify_url = os.environ.get("NOTIFICATION_URL")
        response = requests.post(notify_url, headers=headers, json=notify_data)
        return response.status_code

    return 200


def register_energy_alert_task(data: Dict[str, str]) -> str:
    client = tasks_v2.CloudTasksClient()

    PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
    QUEUE_LOCATION = os.environ.get("CLOUD_TASK_LOCATION")
    QUEUE_ID = os.environ.get("CLOUD_TASK_NAME")
    CREATING_ALERT_URL = os.environ.get("CREATE_ENERGY_ALERT_URL")

    # Time of 2 hours later by default
    in_seconds = data.get("in_seconds", IN_SECONDS)
    d = datetime.datetime.utcnow() + datetime.timedelta(seconds=in_seconds)

    # Create Timestamp protobuf.
    timestamp = timestamp_pb2.Timestamp()
    timestamp.FromDatetime(d)

    parent = client.queue_path(PROJECT_ID, QUEUE_LOCATION, QUEUE_ID)
    url_task_target = data.get("url", CREATING_ALERT_URL)
    task = {
        "http_request": {  # Specify the type of request.
            "http_method": tasks_v2.HttpMethod.POST,
            "url": url_task_target,  # The full url path that the task will be sent to.
            "headers": {"Content-type": "application/json"},
        },
        "schedule_time": timestamp,
    }

    payload = json.dumps(data)
    converted_payload = payload.encode()
    task["http_request"]["body"] = converted_payload
    response = client.create_task(request={"parent": parent, "task": task})

    return response.name
