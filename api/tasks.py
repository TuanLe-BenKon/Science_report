import os
from uuid import UUID
from flask import Response

from sqlalchemy import create_engine

THRESHOLD = 1000


def get_device_data(device_id: UUID, user_id: int, init_timestamp: int) -> Response:
    URL = os.environ.get("SOURCE_DATABASE_URL")
    engine = create_engine(URL).connect()

    return "hello"


def energy_alert(
    start_timestamp: int, current_timestamp: int, current_power: int
) -> bool:
    if current_timestamp >= start_timestamp + 60 * 60 * 2:
        if current_power > THRESHOLD:
            return True

    return False
