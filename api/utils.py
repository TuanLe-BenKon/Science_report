from flask import Response, jsonify
import pandas as pd
import time
import io
from matplotlib.figure import Figure
from google.cloud import storage

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText

from process_data.get_information import *


def message_resp(text: str = "succeeded", status_code: int = 200) -> Response:
    return jsonify(msg=text), status_code


def convert_to_unix_timestamp(_time: str) -> int:
    __time = pd.to_datetime(_time)
    return int(time.mktime(__time.timetuple()))


def send_mail(user_id: int, list_mail: list[str]) -> None:
    mail_content = '''
            Kính gửi quý khách hàng, \n
            Đây là email tự động từ hệ thống BenKon AI Care, quý khách hàng vui lòng liên hệ nhân viên BenKon để được hỗ trợ tốt nhất. 
            Các khái niệm được dùng trong report:
            • Electricity Index (Wh): Chỉ số điện năng, tương tự như chỉ số điện của công tơ điện tử dùng để đo đếm điện năng tiêu thụ của máy điều hoà.
            • Power (W): Công suất tức thời, cho biết mức độ tiêu hao năng lượng của máy điều hoà .
            • Temperature (°C): Nhiệt độ phòng tại vị trí gắn cảm biến (gần máy điều hoà).
            • Humidity (%): Độ ẩm phòng tại vị trí gắn cảm biến (gần máy điều hoà). \n
            Cám ơn quý khách hàng đã sử dụng dịch vụ Quản lý sử dụng điều hoà hiệu quả của BenKon.
        '''

    # The mail addresses and password
    sender_address = 'benkon.cs@lab2lives.com'
    sender_pass = 'BenKonCS@123'

    testing_mail = ['minhdat.bk@gmail.com',
                    'hieu.tran@lab2lives.com',
                    'tuan.le@lab2lives.com'
                    ]
    bcc = ['camp-testing-aaaaexabidfwdrbv3lndltt7q4@lab2lives.slack.com']

    receiver_address = testing_mail
    # receiver_address = list_mail

    # Set up the MIME
    message = MIMEMultipart()
    message['From'] = sender_address
    message['To'] = ", ".join(receiver_address)
    message['Subject'] = f'BenKon Daily Report - {username[user_id]}'

    # The body and the attachments for the mail
    message.attach(MIMEText(mail_content, 'plain'))

    with open(f'./templates/report/BenKon_Daily_Report.pdf', 'rb') as f:
        attach = MIMEApplication(f.read(), _subtype="pdf")
    attach.add_header('Content-Disposition', 'attachment',
                      filename=f'BenKon Daily Report - {username[user_id]}')
    message.attach(attach)

    # Create SMTP session for sending the mail
    session = smtplib.SMTP('smtp.gmail.com', 587)
    session.starttls()
    session.login(sender_address, sender_pass)
    text = message.as_string()
    session.sendmail(sender_address, receiver_address + bcc, text)
    session.quit()

    print('Mail Sent')


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