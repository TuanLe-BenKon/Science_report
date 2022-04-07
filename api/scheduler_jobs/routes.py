from urllib import request
from flask import Blueprint, jsonify, request
from marshmallow import ValidationError
from api.scheduler_jobs.controllers import create_scheduler_job
from api.scheduler_jobs.validation import SchedulerlSchema

from api.utils import message_resp

scheduler_bp = Blueprint("scheduler_bp", __name__)


@scheduler_bp.route("/", methods=["POST"])
def create_scheduler():
    request_data = request.json
    schema = SchedulerlSchema()
    try:
        data = schema.load(request_data)
        user_id = data["user_id"]
        create_scheduler_job(user_id)
        return message_resp()
    except ValidationError as err:
        return message_resp(err.messages, 400)
