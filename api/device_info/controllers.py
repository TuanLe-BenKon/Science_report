from api.device_info.db import get_db


def insert_device_info(
    user_id, user_name, device_id, device_name, status, outdoor_unit
):
    db = get_db()
    cursor = db.cursor()
    statement = "INSERT INTO device_info(user_id, user_name, device_id, device_name, status, outdoor_unit) VALUES (?, ?, ?, ?, ?, ?)"
    cursor.execute(
        statement, [user_id, user_name, device_id, device_name, status, outdoor_unit]
    )
    db.commit()
    return True


def update_device_info(
    user_id, user_name, device_id, device_name, status, outdoor_unit
):
    db = get_db()
    cursor = db.cursor()
    statement = "UPDATE device_info SET user_id = ?, user_name = ?,	device_id = ?, device_name = ?,	status = ?,	outdoor_unit = ? WHERE id = ?"
    cursor.execute(
        statement, [user_id, user_name, device_id, device_name, status, outdoor_unit]
    )
    db.commit()
    return True


def delete_device_info(id):
    db = get_db()
    cursor = db.cursor()
    statement = "DELETE FROM device_info WHERE id = ?"
    cursor.execute(statement, [id])
    db.commit()
    return True


def get_by_id(id):
    db = get_db()
    cursor = db.cursor()
    statement = "SELECT user_id, user_name, device_id, device_name, status, outdoor_unit FROM device_info WHERE id = ?"
    cursor.execute(statement, [id])
    return cursor.fetchone()


def get_device_info():
    db = get_db()
    cursor = db.cursor()
    query = "SELECT user_id, user_name,	device_id, device_name,	status,	outdoor_unit FROM device_info;"
    cursor.execute(query)
    return cursor.fetchall()
