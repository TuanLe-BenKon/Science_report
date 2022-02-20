from dotenv import load_dotenv, find_dotenv
from werkzeug.exceptions import HTTPException
from marshmallow import Schema, fields, ValidationError
from flask import Flask, render_template, jsonify, request, Response

from api.tasks import energy_alert, register_energy_alert_task
from api.errors import TaskCreatedError
from api.utils import message_resp

app = Flask(__name__)
load_dotenv(find_dotenv())


class EnergyAlertTaskSchema(Schema):
    device_id = fields.String(required=True)
    user_id = fields.Integer(required=True)
    init_timestamp = fields.Integer(required=True)


@app.route("/")
def hello():
    return render_template("home.html")


@app.route("/health/")
def health():
    return message_resp()


@app.route(
    "/v1/energy-alert-handler/<string:device_id>/<string:user_id>/<int:init_timestamp>/",
    methods=["GET"],
)
def alert(device_id, user_id, init_timestamp):

    resp = energy_alert(device_id, user_id, init_timestamp)
    return jsonify(msg=resp), 200


@app.route("/v1/energy-alert-task/", methods=["POST"])
def energy_alert_task():
    request_data = request.json
    schema = EnergyAlertTaskSchema()
    try:
        valid_data = schema.load(request_data)
        register_energy_alert_task(valid_data)
        return message_resp()
    except TaskCreatedError as err:
        return message_resp(err.messages, 400)
    except ValidationError as err:
        return message_resp(err.messages, 400)


@app.errorhandler(Exception)
def handle_error(e):
    code = 400
    if isinstance(e, HTTPException):
        code = e.code
    return jsonify(error="Something went wrong"), code
