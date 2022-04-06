from urllib import request
from flask import Blueprint, jsonify, request
from marshmallow import ValidationError
from api.scheduler_jobs.controllers import create_scheduler_job

from api.utils import message_resp

scheduler_bp = Blueprint("scheduler_bp", __name__)


@scheduler_bp.route("/", methods=["POST"])
def create_scheduler():
    data = request.args
    res = create_scheduler_job()
    return res
