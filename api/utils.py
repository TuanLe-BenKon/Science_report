from typing import List
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
from process_data.get_information import *

power = {True: "ON", False: "OFF", None: "Không có"}


def message_resp(text: str = "succeeded", status_code: int = 200) -> Response:
    return jsonify(msg=text), status_code


def get_device_list(user_id):
    df_device_list = df_info[df_info["user_id"] == user_id]
    return df_device_list["device_id"].tolist()


def gen_report(direct: str, user_id: int, track_day: str) -> None:
    device_id_list = get_device_list(user_id)

    total_energy_consumption = 0
    filter_energy = 0

    energy_list = []
    label_list = []
    data = []

    if os.path.exists(direct + "/images/chart/"):
        shutil.rmtree(direct + "./images/chart/")

    os.makedirs(direct + "/images/chart/", exist_ok=True)
    for device_id in device_id_list:
        print(device_name[device_id])
        date = pd.to_datetime(track_day)

        df_sensor, df_energy, df_activities, df_missing = extract_user_data(
            user_id, device_id, track_day
        )

        if (
            len(df_sensor) == 0
            or len(df_energy) == 0
            or np.isnan(df_energy["energy"].max())
        ):
            pass
        else:
            energy_consumption = get_energy_consumption(df_energy) / 1000
            if (
                np.isnan(energy_consumption)
                or int(
                    df_info[df_info["device_id"] == device_id].iloc[0]["outdoor_unit"]
                )
                == 0
            ):
                pass
            else:
                total_energy_consumption += get_energy_consumption(df_energy)
                filter_energy += calc_energy_saving(df_energy, track_day)
                energy_list.append(get_energy_consumption(df_energy) / 1000)
                label_list.append(device_name[device_id])

            export_chart(
                bg_dir, user_id, device_id, df_sensor, df_energy, df_activities, date
            )

        activities = []
        for i in range(len(df_activities)):
            _time = df_activities["timestamp"].iloc[i]
            act_time = "{:02d}:{:02d}:{:02d}".format(
                _time.hour, _time.minute, _time.second
            )
            row_act = ACActivity(
                type=df_activities["event_type"].iloc[i],
                power_status=power[df_activities["power"].iloc[i]],
                op_mode=df_activities["operation_mode"].iloc[i],
                op_time=act_time,
                configured_temp=df_activities["temperature"].iloc[i],
                fan_speed=df_activities["fan_speed"].iloc[i],
            )
            activities.append(row_act)

        chart_url = f"{bg_dir}/chart_{device_name[device_id]}.png"
        if not os.path.exists(chart_url):
            chart_url = ""

        data_report = BenKonReportData(
            user=username[user_id],
            device=device_name[device_id],
            report_date=pd.to_datetime(track_day),
            chart_url=chart_url,
            energy_kwh=np.round(get_energy_consumption(df_energy) / 1000, 3),
            activities=activities,
        )
        data.append(data_report)

    if len(energy_list) == 0 or len(label_list) == 0:
        pass
    else:
        if total_energy_consumption == 0:
            saving_percent = 0
        else:
            saving_percent = np.round(
                100 - (filter_energy * 100) / total_energy_consumption, 2
            )

        # export_pie_chart_energy_consumption(bg_dir, user_id, track_day, label_list, energy_list, saving_percent)
        # export_energy_consumption_and_working_time_chart(bg_dir, user_id, device_id_list, track_day)

    os.makedirs(f"./templates/report/", exist_ok=True)
    report = BenKonReport("./templates/report/BenKon_Daily_Report.pdf", data=data)


def send_mail(user_id: int, list_mail: List[str]) -> None:
    mail_content = """
            Kính gửi quý khách hàng, \n
            Đây là email tự động từ hệ thống BenKon AI Care, quý khách hàng vui lòng liên hệ nhân viên BenKon để được hỗ trợ tốt nhất. 
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
    # receiver_address = list_mail

    # Set up the MIME
    message = MIMEMultipart()
    message["From"] = sender_address
    message["To"] = ", ".join(receiver_address)
    message["Subject"] = f"BenKon Daily Report - {username[user_id]}"

    # The body and the attachments for the mail
    message.attach(MIMEText(mail_content, "plain"))

    with open(f"./templates/report/BenKon_Daily_Report.pdf", "rb") as f:
        attach = MIMEApplication(f.read(), _subtype="pdf")
    attach.add_header(
        "Content-Disposition",
        "attachment",
        filename=f"BenKon Daily Report - {username[user_id]}",
    )
    message.attach(attach)

    # Create SMTP session for sending the mail
    session = smtplib.SMTP("smtp.gmail.com", 587)
    session.starttls()
    session.login(sender_address, sender_pass)
    text = message.as_string()
    session.sendmail(sender_address, receiver_address, text)
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
