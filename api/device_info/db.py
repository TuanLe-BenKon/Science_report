import sqlite3
from sqlite3 import Connection

DATABASE_NAME = "device_info.db"


def get_db() -> Connection:
    conn = sqlite3.connect(DATABASE_NAME)
    return conn


def create_tables() -> None:
    tables = [
        """CREATE TABLE IF NOT EXISTS device_info(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                user_name TEXT NOT NULL,
                device_id TEXT NOT NULL,
                device_name TEXT NOT NULL,
				status BOOLEAN NOT NULL,
				outdoor_unit BOOLEAN NOT NULL
            )
            """
    ]
    db = get_db()
    cursor = db.cursor()
    for table in tables:
        cursor.execute(table)
