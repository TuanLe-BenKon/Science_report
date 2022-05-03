from urllib import request
from flask import Blueprint, jsonify, request
from marshmallow import ValidationError

from api.device_info.controllers import (
    get_device_info,
    insert_device_info,
    delete_device_info,
)
from api.device_info.validation import DeviceInfoSchema
from api.utils import message_resp

device_bp = Blueprint("device_bp", __name__)


@device_bp.route("/", methods=["GET"])
def get_device():
    device_info = get_device_info()
    return jsonify(device_info)


@device_bp.route("/", methods=["POST"])
def insert_device():
    request_data = request.json
    schema = DeviceInfoSchema()
    try:
        data = schema.load(request_data)
        user_id = data["user_id"]
        user_name = data["user_name"]
        device_id = data["device_id"]
        device_name = data["device_name"]
        status = data["status"]
        outdoor_unit = data["outdoor_unit"]
        res = insert_device_info(
            user_id, user_name, device_id, device_name, status, outdoor_unit
        )
        return jsonify(res)
    except ValidationError as err:
        return message_resp(err.messages, 400)


@device_bp.route("/", methods=["DELETE"])
def delete_device():
    data = request.args
    res = delete_device_info(data["id"])
    return res


@device_bp.route("/", methods=["PUT"])
def update_device():
    request_data = request.json
    schema = DeviceInfoSchema()
    try:
        data = schema.load(request_data)
        id = data["id"]
        user_id = data["user_id"]
        user_name = data["user_name"]
        device_id = data["device_id"]
        device_name = data["device_name"]
        status = data["status"]
        outdoor_unit = data["outdoor_unit"]
        res = delete_device_info(
            id, user_id, user_name, device_id, device_name, status, outdoor_unit
        )
        return res
    except ValidationError as err:
        return message_resp(err.messages, 400)
