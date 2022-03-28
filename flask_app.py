import datetime
import os
import time

import shutil
import pandas as pd
from dotenv import load_dotenv, find_dotenv
from werkzeug.exceptions import HTTPException
from marshmallow import Schema, fields, ValidationError
from flask import Flask, render_template, jsonify, request, Response

from api.tasks import energy_alert, register_energy_alert_task
from api.validation_schema import EnergyAlertTaskSchema
from api.utils import message_resp, send_mail

from bkreport import BenKonReport, BenKonReportData, ACActivity
from process_data.extract_user_data import *
from process_data.chart import *
from process_data.get_information import *

app = Flask(__name__)

power = {
    True: 'ON',
    False: 'OFF',
    None: 'Không có'
}

mail_list = [
             # 'an.nguyen@seedcom.vn',
             # 'luan.nguyen@seedcom.vn',
             # 'thinh.huynhhuy@thecoffeehouse.vn',
             # 'hai.hoang@thecoffeehouse.vn',
             # 'phuong.huynhtuan@thecoffeehouse.vn'
             ]


def get_device_list(user_id):
    df_device_list = df_info[df_info['user_id'] == user_id]
    return df_device_list['device_id'].tolist()


@app.route("/science/dailyReport")
def dailyReport():
    user_id = int(request.args.get('user_id'))
    track_day = request.args.get('track_day')

    device_id_list = get_device_list(user_id)

    total_energy_consumption = 0
    filter_energy = 0

    energy_list = []
    label_list = []
    data = []

    if os.path.exists('./images/chart/'):
        shutil.rmtree('./images/chart/')

    os.makedirs('./images/chart/', exist_ok=True)
    for device_id in device_id_list:
        print(device_name[device_id])
        date = pd.to_datetime(track_day)

        df_sensor, df_energy, df_activities, df_missing = extract_user_data(user_id, device_id, track_day)

        if len(df_sensor) == 0 or len(df_energy) == 0 or np.isnan(df_energy['energy'].max()):
            pass
        else:
            energy_consumption = get_energy_consumption(df_energy) / 1000
            if np.isnan(energy_consumption) or int(df_info[df_info['device_id'] == device_id].iloc[0]['outdoor_unit']) == 0:
                pass
            else:
                total_energy_consumption += get_energy_consumption(df_energy)
                filter_energy += calc_energy_saving(df_energy, track_day)
                energy_list.append(get_energy_consumption(df_energy) / 1000)
                label_list.append(device_name[device_id])

            export_chart(bg_dir, user_id, device_id, df_sensor, df_energy, df_activities, date)

        activities = []
        for i in range(len(df_activities)):
            _time = df_activities['timestamp'].iloc[i]
            act_time = '{:02d}:{:02d}:{:02d}'.format(_time.hour, _time.minute, _time.second)
            row_act = ACActivity(
                type=df_activities['event_type'].iloc[i],
                power_status=power[df_activities['power'].iloc[i]],
                op_mode=df_activities['operation_mode'].iloc[i],
                op_time=act_time,
                configured_temp=df_activities['temperature'].iloc[i],
                fan_speed=df_activities['fan_speed'].iloc[i],
            )
            activities.append(row_act)

        chart_url = f'{bg_dir}/chart_{device_name[device_id]}.png'
        if not os.path.exists(chart_url):
            chart_url = ''

        data_report = BenKonReportData(
            user=username[user_id],
            device=device_name[device_id],
            report_date=pd.to_datetime(track_day),
            chart_url=chart_url,
            energy_kwh=np.round(get_energy_consumption(df_energy) / 1000, 3),
            activities=activities
        )
        data.append(data_report)

    if len(energy_list) == 0 or len(label_list) == 0:
        pass
    else:
        if total_energy_consumption == 0:
            saving_percent = 0
        else:
            saving_percent = np.round(100 - (filter_energy * 100) / total_energy_consumption, 2)

        # export_pie_chart_energy_consumption(bg_dir, user_id, track_day, label_list, energy_list, saving_percent)
        # export_energy_consumption_and_working_time_chart(bg_dir, user_id, device_id_list, track_day)

    os.makedirs(f'./templates/report/', exist_ok=True)
    report = BenKonReport('./templates/report/BenKon_Daily_Report.pdf', data=data)

    send_mail(user_id, mail_list)

    return 'EMAIL HAS BEEN SENT'


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
