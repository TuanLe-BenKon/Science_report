import os
import requests
import datetime
from process_data.get_information import *


def download_data(bg_dir, user_id, device_id_list, start_day, end_day):

    response = requests.get('https://admin.benkon.io/admins/sign_in', headers=headers, cookies=cookies)

    if response.status_code == 200:
        print('[LOG] Login success')

        with requests.Session() as session:

            date_start = pd.to_datetime(start_day)

            date_end = pd.to_datetime(end_day)

            index = 1

            for device_id in device_id_list:

                date = date_start
                print(device_name[device_id])

                os.makedirs(os.path.dirname('{}/{}/{}/chart/'.format(bg_dir, username[user_id], device_name[device_id])), exist_ok=True)

                while True:

                    # Export sensor data
                    url_sensor = 'https://admin.benkon.io/devices/export-sensor-data?date={}-{:02d}-{:02d}&id={}'.format(
                        date.year,
                        date.month,
                        date.day,
                        device_id)

                    csv_file_response = session.get(url_sensor, cookies=cookies)
                    csv_file_name_sensor = '{}/{}/{}/{}-{:02d}-{:02d}/sensor_data-{}-{:02d}-{:02d}.csv'.format(
                        bg_dir, 
                        username[user_id], 
                        device_name[device_id],
                        date.year, 
                        date.month,
                        date.day,
                        date.year, 
                        date.month,
                        date.day)
                    os.makedirs(os.path.dirname(csv_file_name_sensor), exist_ok=True)
                    with open(csv_file_name_sensor, 'w', encoding='utf-8') as f:
                        f.write(csv_file_response.text)

                    # ---------------------------------------------------------------------------------------------- #
                    # Export energy data
                    url_energy = 'https://admin.benkon.io/devices/export-energy-data?date={}-{:02d}-{:02d}&id={}'.format(
                        date.year,
                        date.month,
                        date.day,
                        device_id)

                    csv_file_response = session.get(url_energy, cookies=cookies)
                    csv_file_name_energy = '{}/{}/{}/{}-{:02d}-{:02d}/energy_data-{}-{:02d}-{:02d}.csv'.format(
                        bg_dir, 
                        username[user_id], 
                        device_name[device_id],
                        date.year, 
                        date.month,
                        date.day,
                        date.year, 
                        date.month,
                        date.day)
                    os.makedirs(os.path.dirname(csv_file_name_energy), exist_ok=True)
                    with open(csv_file_name_energy, 'w', encoding='utf-8') as f:
                        f.write(csv_file_response.text)

                    # ---------------------------------------------------------------------------------------------- #
                    # Export activities'
                    url_activities = 'https://admin.benkon.io/devices/export-activities?date={}-{:02d}-{:02d}&id={}'.format(
                        date.year,
                        date.month,
                        date.day,
                        device_id)

                    csv_file_response = session.get(url_activities, cookies=cookies)
                    csv_file_name_energy = '{}/{}/{}/{}-{:02d}-{:02d}/activities-{}-{:02d}-{:02d}.csv'.format(
                        bg_dir, 
                        username[user_id], 
                        device_name[device_id],
                        date.year, 
                        date.month,
                        date.day,
                        date.year, 
                        date.month,
                        date.day)

                    os.makedirs(os.path.dirname(csv_file_name_energy), exist_ok=True)
                    with open(csv_file_name_energy, 'w', encoding='utf-8') as f:
                        f.write(csv_file_response.text)

                    # ---------------------------------------------------------------------------------------------- #
                    # Increase day
                    date += datetime.timedelta(days=1)
                    if date > date_end:
                        break

                print('[LOG] Completed {:d} device'.format(index))
                index += 1
    else:
        print('[LOG] Failed')

    print('[LOG] Completed get data from cloud for user ' + username[user_id] + '\n')


cookies = {
    '_ga': 'GA1.1.1953815592.1642998514',
    '_fbp': 'fb.1.1645503068612.1864184549',
    '_ga_841YEL5CLX': 'GS1.1.1645503068.4.1.1645503209.0',
    'remember_admin_token': 'eyJfcmFpbHMiOnsibWVzc2FnZSI6Ilcxc3hYU3dpSkRKaEpERXlKRFF5U0RsS1JrSjZjVWRIUWxwcWVFd3VSemhrT0M0aUxDSXhOalExTlRnNE16QTFMakUyTVRVMU5UTWlYUT09IiwiZXhwIjoiMjAyMi0wMy0wOVQwMzo1MTo0NS4xNjFaIiwicHVyIjoiY29va2llLnJlbWVtYmVyX2FkbWluX3Rva2VuIn19--ee25d619822118a2e6f5fdcd9650da1f5db0fac8',
    '_cf_appbe_session': 'hCUKOxPtMnxfWfPfrI82cgnTB3rKZQVlb6ARNuEkWhlfmZqg8w91ddcw325SKMefxP1D%2FrEVDXezlko9WG7rVJkH%2FJSk7ZtaGJDXyM6D5zGnkXNMfHN2%2BWyo3E%2BI00szXkim28J0xXajWmeXSKJXkp0PB5dYoFZqTGO%2FbyDq9RZWsLNJQqX%2FJYiTujZgTPShNbxGtDgOC2BCq9XPL7C4dKAwnA5WCCdYjelJuWZjJkspsW7TCk9aGego9mUo%2BMt%2B5dR81fiHcF3rr192ws6BzFdN%2B%2FCjvbwZjRwauLX7bDs9uhS8lrhaotEbY7cHMjjMK78Hm7VNMzO1%2FCOMH%2BxBsYkGipfp746eJe2YQJwC1THpLUhAFcpM%2BbmoonTXBDvH61E1QQN8jZi0E%2FF3vbKuXOyieIPjT5fSHQ%3D%3D--Jq4YpButlwuraJwh--%2BWltDlI3LPHwrUN7Y4MldQ%3D%3D',
}

headers = {
    'Connection': 'keep-alive',
    'Cache-Control': 'max-age=0',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-User': '?1',
    'Sec-Fetch-Dest': 'document',
    'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="98", "Google Chrome";v="98"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'Referer': 'https://admin.benkon.io/admins/sign_in',
    'Accept-Language': 'en-US,en;q=0.9',
}
