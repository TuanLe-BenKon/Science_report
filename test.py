import requests
import datetime

date = datetime.datetime.now() - datetime.timedelta(days=1)
track_day = '{}-{:02d}-{:02d}'.format(date.year, date.month, date.day)

# ids = ['10019', '11294', '11296']
ids = ['11296']

for _id in ids:
    res = requests.get(f'http://192.168.1.3:8080/science/v1/daily-report')
    print(res.text)

