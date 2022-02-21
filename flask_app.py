import os
from dotenv import load_dotenv, find_dotenv
from werkzeug.exceptions import HTTPException
from marshmallow import Schema, fields, ValidationError
from flask import Flask, render_template, jsonify, request, Response

from api.tasks import energy_alert, register_energy_alert_task
from api.validation_schema import EnergyAlertTaskSchema
from api.utils import message_resp

app = Flask(__name__)


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
        tast_name = register_energy_alert_task(valid_data)
        return message_resp("Created task {}".format(tast_name), 201)
    except ValidationError as err:
        return message_resp(err.messages, 400)


@app.errorhandler(Exception)
def global_error_handler(e):
    code = 400
    if isinstance(e, HTTPException):
        code = e.code
    return jsonify(error="Something went wrong"), code


if __name__ == "__main__":
    load_dotenv(find_dotenv())

    server_port = os.environ.get("PORT", "8080")
    app.run(debug=False, port=server_port, host="0.0.0.0")
