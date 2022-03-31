from api.customer_emails.db import get_db


def insert_customer_emails(user_id, user_name, external, internal):
    db = get_db()
    cursor = db.cursor()
    statement = "INSERT INTO customer_emails(user_id, user_name, external, internal) VALUES (?, ?, ?, ?)"
    cursor.execute(statement, [user_id, user_name, external, internal])
    db.commit()
    return True


def update_customer_emails(id, user_id, user_name, external, internal):
    db = get_db()
    cursor = db.cursor()
    statement = "UPDATE customer_emails SET user_id = ?, user_name = ?,	external = ?, internal = ? WHERE id = ?"
    cursor.execute(
        statement, [user_id, user_name, external, internal, id],
    )
    db.commit()
    return True


def delete_customer_emails(id):
    db = get_db()
    cursor = db.cursor()
    statement = "DELETE FROM customer_emails WHERE id = ?"
    cursor.execute(statement, [id])
    db.commit()
    return True


def get_by_id(id):
    db = get_db()
    cursor = db.cursor()
    statement = "SELECT user_id, user_name, external, internal FROM customer_emails WHERE id = ?"
    cursor.execute(statement, [id])
    return cursor.fetchone()


def get_customer_emails():
    db = get_db()
    cursor = db.cursor()
    query = "SELECT id, user_id, user_name, external, internal FROM customer_emails;"
    cursor.execute(query)
    return cursor.fetchall()
