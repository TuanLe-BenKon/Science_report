import sqlite3
from sqlite3 import Connection

DATABASE_NAME = "customer_emails.db"


def get_db() -> Connection:
    conn = sqlite3.connect(DATABASE_NAME)
    return conn


def create_tables() -> None:
    tables = [
        """CREATE TABLE IF NOT EXISTS customer_emails(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                user_name TEXT NOT NULL,
                external TEXT NOT NULL,
                internal TEXT NOT NULL
            )
            """
    ]
    db = get_db()
    cursor = db.cursor()
    for table in tables:
        cursor.execute(table)
