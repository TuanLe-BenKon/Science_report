import os

import pandas as pd
from dotenv import load_dotenv, find_dotenv
from werkzeug.exceptions import HTTPException
from marshmallow import ValidationError
from flask import Flask, render_template, jsonify, request
from api.device_info.db import create_tables

from api.tasks import energy_alert, register_energy_alert_task
from api.validation_schema import EnergyAlertTaskSchema, GenReportSchema
from api.utils import message_resp, send_mail, gen_report
from api.device_info.routes import device_bp
from api.device_info.controllers import *


app = Flask(__name__)
app.register_blueprint(device_bp, url_prefix="/science/device")

mail_list = ["nhat.thai@lab2lives.com"]


@app.route("/science/")
def hello():
    return render_template("home.html")


@app.route("/science/v1/daily-report", methods=["GET"])
def dailyReport():
    records = get_device_info()
    df_info = pd.DataFrame(
        records,
        columns=['no', 'user_id', 'user_name', 'device_id', 'device_name', 'status', 'outdoor_unit']
    )
    df_info = df_info.drop(columns='no')

    request_data = request.args
    schema = GenReportSchema()
    try:
        data = schema.load(request_data)
        user_id = data["user_id"]
        track_day = data["track_day"]
    except ValidationError as err:
        return message_resp(err.messages, 400)

    gen_report(df_info, user_id, track_day)
    send_mail(df_info, user_id, track_day, mail_list)

    return message_resp()


@app.route("/science/health/")
def health():
    return message_resp()


@app.route(
    "/science/v1/energy-alert-handler", methods=["POST"],
)
def alert():
    request_data = request.json
    schema = EnergyAlertTaskSchema()
    try:
        valid_data = schema.load(request_data)
        status_code = energy_alert(valid_data)
        return message_resp("succeed", status_code)
    except ValidationError as err:
        return message_resp(err.messages, 400)


@app.route("/science/v1/energy-alert-task", methods=["POST"])
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
    create_tables()
    server_port = os.environ.get("PORT", "8080")
    app.run(debug=False, port=server_port, host="0.0.0.0")

