import datetime
import json
import os
from uuid import UUID
from typing import Dict

import pandas as pd
from flask import Response
from google.cloud import tasks_v2
from sqlalchemy import create_engine
from google.protobuf import timestamp_pb2

THRESHOLD = 1000
IN_SECONDS = 7200  # 2 hours in seconds
DATABASE_URL = os.environ.get("SOURCE_DATABASE_URL")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
QUEUE_LOCATION = os.environ.get("CLOUD_TASK_LOCATION")
QUEUE_ID = os.environ.get("CLOUD_TASK_NAME")
CREATING_ALERT_URL = os.environ.get("CREATE_ENERGY_ALERT_URL")


def get_device_data(device_id: UUID, user_id: int, init_timestamp: int) -> Response:
    engine = create_engine(DATABASE_URL)
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


def energy_alert(device_id: UUID, user_id: int, init_timestamp: int) -> str:
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


def register_energy_alert_task(data: Dict[str, str]) -> str:
    client = tasks_v2.CloudTasksClient()
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
