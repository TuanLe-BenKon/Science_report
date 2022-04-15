import os
from typing import List, Dict, Any

import numpy as np
import pandas as pd
from flask import Response, jsonify
import io
from matplotlib.figure import Figure
from google.cloud import storage
import shutil

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText

from bkreport import BenKonReport, BenKonReportData, ACActivity
from process_data.extract_user_data import *
from process_data.chart import *

power = {True: "ON", False: "OFF", None: "Không có"}
local_chart_dir = os.path.join(os.getcwd(), "/tmp/chart")
local_report_dir = os.path.join(os.getcwd(), "/tmp/report")


def get_username(df_info: pd.DataFrame) -> Dict[Any, Any]:
    # Convert user_id and user_name to dict type
    df_username = df_info[["user_id", "user_name"]]
    df_username = df_username.drop_duplicates().reset_index(drop=True)
    username = dict(zip(df_username["user_id"], df_username["user_name"]))
    return username


def get_device_name(df_info: pd.DataFrame) -> Dict[Any, Any]:
    # Convert device_id and device_name to dict type
    df_device_name = df_info[df_info["status"] == 1]
    df_device_name = df_device_name[["device_id", "device_name"]]
    df_device_name = df_device_name.drop_duplicates().reset_index(drop=True)
    device_name = dict(zip(df_device_name["device_id"], df_device_name["device_name"]))
    return device_name


def get_device_list(df_info: pd.DataFrame, user_id: str) -> List[str]:
    df_device_list = df_info[(df_info["user_id"] == user_id) & df_info["status"] == 1]
    return df_device_list["device_id"].tolist()


def message_resp(text: str = "succeeded", status_code: int = 200) -> Response:
    return jsonify(msg=text), status_code


def gen_report(df_info: pd.DataFrame, user_id: str, track_day: str) -> None:
    username = get_username(df_info)
    device_name = get_device_name(df_info)
    device_id_list = get_device_list(df_info, user_id)

    energy_list = []
    device_list = []
    data = []

    if os.path.exists(local_chart_dir):
        shutil.rmtree(local_chart_dir)

    os.makedirs(local_chart_dir, exist_ok=True)
    for device_id in device_id_list:
        print(device_name[device_id])

        date = pd.to_datetime(track_day)

        df_sensor, df_energy, df_activities = extract_user_data(
            user_id, device_id, track_day
        )

        if (
            (df_sensor.empty and df_energy.empty) or
            np.isnan(df_energy['energy'].min())
        ):
            pass
        else:
            if int(df_info[df_info['device_id'] == device_id]['outdoor_unit']) == 1:
                if (not np.isnan(get_energy_consumption(df_energy))) and (get_energy_consumption(df_energy) > 1000):
                    energy_list.append(get_energy_consumption(df_energy)/1000)
                    device_list.append(device_id)

            export_chart(
                local_chart_dir,
                device_name,
                device_id,
                df_sensor,
                df_energy,
                df_activities,
                date,
            )

        # Load AC's activities to list
        activities = []
        for i in range(len(df_activities)):

            # Convert time
            _time = df_activities["timestamp"].iloc[i]
            act_time = "{:02d}:{:02d}:{:02d}".format(
                _time.hour, _time.minute, _time.second
            )

            # If event_type relates to scheduler
            if (
                    df_activities["event_type"].iloc[i] == "delete_scheduler"
            ):
                row_act = ACActivity(
                    type=df_activities["event_type"].iloc[i],
                    power_status=power[df_activities["power"].iloc[i]],
                    op_mode="Không có",
                    op_time=act_time,
                    configured_temp="Không có",
                    fan_speed="Không có",
                )
            else:

                # Fan Speed
                if df_activities["fan_speed"].iloc[i] == 7:
                    fan_speed = "Auto"
                else:
                    fan_speed = str(int(df_activities["fan_speed"].iloc[i]))

                # Temperature
                if power[df_activities["power"].iloc[i]] in ['OFF', 'Không có']:
                    configured_temp = '-- °C'
                else:
                    configured_temp = str(int(df_activities["temperature"].iloc[i])) + "°C"

                row_act = ACActivity(
                    type=df_activities["event_type"].iloc[i],
                    power_status=power[df_activities["power"].iloc[i]],
                    op_mode=df_activities["operation_mode"].iloc[i],
                    op_time=act_time,
                    configured_temp=configured_temp,
                    fan_speed=fan_speed,
                )
            activities.append(row_act)

        # Get chart URL
        chart_url = f"{local_chart_dir}/chart_{device_name[device_id]}.png"
        if not os.path.exists(chart_url):
            chart_url = ""

        # Gen report's page for each device
        data_report = BenKonReportData(
            user=username[user_id],
            device=device_name[device_id],
            report_date=pd.to_datetime(track_day),
            chart_url=chart_url,
            energy_kwh=np.round(get_energy_consumption(df_energy) / 1000, 3),
            activities=activities,
        )
        data.append(data_report)

    total_energy_consumption = float(np.sum(energy_list))
    if len(device_list) <= 8:
        pass
    else:
        df = pd.DataFrame(data=zip(device_list, energy_list), columns=['device_id', 'energy_consumption'])
        df = df.sort_values(by='energy_consumption', ascending=False).reset_index(drop=True)
        device_list = df.iloc[:8]['device_id'].to_list()
        energy_list = df.iloc[:8]['energy_consumption'].to_list()

    # Summary page information
    if len(energy_list) == 0 or len(device_list) == 0:
        # Không có device nào có năng lượng
        isGenSummaryPage = False
        pass
    else:
        isGenSummaryPage = True
        export_energy_pie_chart(
            local_chart_dir,
            device_list,
            energy_list,
            total_energy_consumption,
            device_name,
            track_day
        )

        export_last_3_days_working_time(
            local_chart_dir,
            device_list,
            device_name,
            track_day
        )

    # isGenSummaryPage = False

    # Check if pie chart is exists
    url_pie_chart = f'{local_chart_dir}/EnergyPieChart.png'
    if not os.path.exists(url_pie_chart):
        url_pie_chart = ''

    # Check if bar chart is exists
    url_bar_chart = f'{local_chart_dir}/Last3DaysChart.png'
    if not os.path.exists(url_bar_chart):
        url_bar_chart = ''

    os.makedirs(f"{local_report_dir}", exist_ok=True)
    BenKonReport(
        f"{local_report_dir}/BenKon_Daily_Report.pdf",
        isGenSummaryPage=isGenSummaryPage,
        url_pie_chart=url_pie_chart,
        url_bar_chart=url_bar_chart,
        data=data
    )


def send_mail(
        df_info: pd.DataFrame, user_id: str, track_day: str, list_mail: List[str], bcc: List[str]
) -> None:
    username = get_username(df_info)

    mail_content = """
            Kính gửi quý khách hàng, \n
            Đây là email tự động từ hệ thống BenKon AI Care. Quý khách hàng vui lòng liên hệ nhân viên BenKon để được hỗ trợ tốt nhất. 
            Các khái niệm được dùng trong report:
            • Electricity Index (Wh): Chỉ số điện năng, tương tự như chỉ số điện của công tơ điện tử dùng để đo đếm điện năng tiêu thụ của máy điều hoà.
            • Power (W): Công suất tức thời, cho biết mức độ tiêu hao năng lượng của máy điều hoà .
            • Temperature (°C): Nhiệt độ phòng tại vị trí gắn cảm biến (gần máy điều hoà).
            • Humidity (%): Độ ẩm phòng tại vị trí gắn cảm biến (gần máy điều hoà). \n
            Cám ơn quý khách hàng đã sử dụng dịch vụ Quản lý sử dụng điều hoà hiệu quả của BenKon.
        """

    # The mail addresses and password
    sender_address = "benkon.cs@lab2lives.com"
    sender_pass = "BenKonCS@123"

    receiver_address = list_mail

    # Set up the MIME
    message = MIMEMultipart()
    message["From"] = sender_address
    message["To"] = ", ".join(receiver_address)
    message["Subject"] = f"BenKon Daily Report - {username[user_id]} - {track_day}"

    # The body and the attachments for the mail
    message.attach(MIMEText(mail_content, "plain"))

    with open(f"{local_report_dir}/BenKon_Daily_Report.pdf", "rb") as f:
        attach = MIMEApplication(f.read(), _subtype="pdf")
    attach.add_header(
        "Content-Disposition",
        "attachment",
        filename=f"BenKon Daily Report - {username[user_id]}.pdf",
    )
    message.attach(attach)

    # Create SMTP session for sending the mail
    session = smtplib.SMTP("smtp.gmail.com", 587)
    session.starttls()
    session.login(sender_address, sender_pass)
    text = message.as_string()
    session.sendmail(sender_address, receiver_address + bcc, text)
    session.quit()

    print("Mail Sent")


def saving_figure(file_path: str, fig: Figure) -> None:
    buf = io.BytesIO()  # Save figure image to a bytes buffer
    fig.savefig(buf, format="png")
    buf.seek(0)

    client = storage.Client()
    bucket_name = os.environ.get("GCS_BUCKET")
    bucket = client.get_bucket(bucket_name)

    # This defines the path where the file will be stored in the bucket
    blob = bucket.blob(f"reports/{file_path}")
    blob.upload_from_string(buf.getvalue(), content_type="image/png")
