from urllib import request
from flask import Blueprint, jsonify, request
from marshmallow import ValidationError

from api.customer_emails.controllers import (
    get_customer_emails,
    insert_customer_emails,
    delete_customer_emails,
)
from api.customer_emails.validation import CustomerEmailSchema
from api.utils import message_resp

customer_bp = Blueprint("customer_bp", __name__)


@customer_bp.route("/", methods=["GET"])
def get_customer():
    customer_emails = get_customer_emails()
    return jsonify(customer_emails)


@customer_bp.route("/", methods=["POST"])
def insert_customer():
    request_data = request.json
    schema = CustomerEmailSchema()
    try:
        data = schema.load(request_data)
        user_id = data["user_id"]
        user_name = data["user_name"]
        external = data["external"]
        internal = data["internal"]
        res = insert_customer_emails(user_id, user_name, external, internal)
        return jsonify(res)
    except ValidationError as err:
        return message_resp(err.messages, 400)


@customer_bp.route("/", methods=["DELETE"])
def delete_customer():
    data = request.args
    res = delete_customer_emails(data["id"])
    return res


@customer_bp.route("/", methods=["PUT"])
def update_customer():
    request_data = request.json
    schema = CustomerEmailSchema()
    try:
        data = schema.load(request_data)
        id = data["id"]
        user_id = data["user_id"]
        user_name = data["user_name"]
        external = data["external"]
        internal = data["internal"]
        res = delete_customer_emails(id, user_id, user_name, external, internal)
        return res
    except ValidationError as err:
        return message_resp(err.messages, 400)
