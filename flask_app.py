import datetime
import os
import time

import pandas as pd
from dotenv import load_dotenv, find_dotenv
from werkzeug.exceptions import HTTPException
from marshmallow import Schema, fields, ValidationError
from flask import Flask, render_template, jsonify, request, Response

from api.tasks import energy_alert, register_energy_alert_task
from api.validation_schema import EnergyAlertTaskSchema
from api.utils import message_resp, send_mail, gen_report

from process_data.get_information import *

app = Flask(__name__)

mail_list = [
             'an.nguyen@seedcom.vn',
             'luan.nguyen@seedcom.vn',
             'thinh.huynhhuy@thecoffeehouse.vn',
             'hai.hoang@thecoffeehouse.vn',
             'phuong.huynhtuan@thecoffeehouse.vn'
             ]


@app.route("/science/dailyReport")
def dailyReport():

    data = request.args

    user_id = int(data['user_id'])
    track_day = data['track_day']

    direct = os.getcwd()
    gen_report(direct, user_id, track_day)

    send_mail(user_id, mail_list)

    return message_resp()


@app.route("/science/")
def hello():
    return render_template("home.html")


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
    server_port = os.environ.get("PORT", "8080")
    app.run(debug=False, port=server_port, host="0.0.0.0")
