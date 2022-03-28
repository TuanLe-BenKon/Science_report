import os
import requests
import datetime
from datetime import datetime as dt
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
from process_data.get_information import *
from process_data.utils import *


# Draw chart
# Basic chart
def export_chart(bg_dir, user_id, device_id, df_sensor, df_energy, df_activities, date):
    track_day = '{}-{:02d}-{:02d}'.format(date.year, date.month, date.day)

    fig, axs = plt.subplots(3, 1, sharex=True, facecolor='w', figsize=(15, 12))

    # Remove horizontal space between axes
    fig.subplots_adjust(hspace=0.05)

    # Adding grid line
    axs[0].grid(linestyle='--', alpha=0.5)
    axs[1].grid(linestyle='--', alpha=0.5)
    axs[2].grid(linestyle='--', alpha=0.5)

    # Set time limit in one day
    axs[0].set_xlim([date, date + datetime.timedelta(days=1)])

    # Set timestamp display format HH:MM
    # For each xticks, it'll display for each hours of 1 day, respectively
    axs[0].xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    axs[0].xaxis.set_major_locator(mdates.HourLocator(interval=1))

    # Plot each graph, and manually set the y tick values
    ''' Plot Energy and Power '''
    p1 = axs[0].plot(df_energy['timestamp'], df_energy['energy'], color='blue', alpha=0.3, label='energy')
    axs[0].fill_between(df_energy['timestamp'], df_energy['energy'], df_energy['energy'].min(),
                        where=df_energy['energy'] > df_energy['energy'].min(), color='blue', label='energy', alpha=0.1,
                        animated=True, hatch='.', facecolor='w')
    axs[0].set_ylabel('Electricity Index (Wh)', fontsize=14)

    ax1 = axs[0].twinx()
    if df_energy['power'].max() < 1500:
        ax1.set_ylim([0, 1500])

    p2 = ax1.plot(df_energy['timestamp'], df_energy['power'], color='red', label='power')
    ax1.set_ylabel('Power (Watt)', fontsize=14)

    if df_sensor['temperature'].min() > 20 and df_sensor['temperature'].max() < 32:
        axs[1].set_ylim([20, 32])

    axs[1].plot(df_sensor['timestamp'], df_sensor['temperature'], color='green', label='temperature')
    axs[1].set_ylabel(r'Temperature ($^{\circ}C$)', fontsize=14)

    if df_sensor['humidity'].min() > 40 and df_sensor['humidity'].max() < 70:
        axs[2].set_ylim([40, 70])

    axs[2].plot(df_sensor['timestamp'], df_sensor['humidity'], color='y', label='humidity')
    axs[2].set_ylabel('Humidity (%)', fontsize=14)

    p = p1 + p2
    labs = [plot.get_label() for plot in p]

    for i in range(len(df_activities)):
        axs[0].axvline(x=df_activities['timestamp'].iloc[i], label='activity' + str(i), alpha=0.5, color='navy')
        axs[1].axvline(x=df_activities['timestamp'].iloc[i], label='activity' + str(i), alpha=0.5, color='navy')
        axs[2].axvline(x=df_activities['timestamp'].iloc[i], label='activity' + str(i), alpha=0.5, color='navy')

        flag_sch = False
        if df_activities['event_type'].iloc[i] == 'update_scheduler':
            control = 'UpSch_' + str(i)
            flag_sch = True
        elif df_activities['event_type'].iloc[i] == 'add_scheduler':
            control = 'AddSch_' + str(i)
            flag_sch = True
        elif df_activities['event_type'].iloc[i] == 'delete_scheduler':
            control = 'DelSch_' + str(i)
            flag_sch = True
        elif df_activities['event_type'].iloc[i] == 'remote_control':
            control = 'Remote_' + str(i)
        elif df_activities['event_type'].iloc[i] == 'set_temperature' or df_activities['event_type'].iloc[
            i] == 'set_operation_mode':
            control = 'App_' + str(i)
        elif df_activities['event_type'].iloc[i] == 'scheduler_control':
            control = 'Sche_' + str(i)
        else:
            control = 'App_' + str(i)

        if df_activities['power'].iloc[i]:
            power = 'ON'
        else:
            power = 'OFF'

        if not flag_sch:
            act_info = '''{}\n{}\n{}\n{}°C\nfan_{}
                '''.format(power, control, df_activities['operation_mode'].iloc[i],
                           str(df_activities['temperature'].iloc[i]),
                           int(df_activities['fan_speed'].iloc[i]))
        else:
            act_info = control

        axs[2].text(((df_activities['timestamp'].iloc[i] - date).total_seconds() - 900) / 86400, -1.15,
                    act_info,
                    horizontalalignment='left',
                    verticalalignment='top',
                    transform=axs[1].transAxes,
                    bbox={'alpha': 1, 'pad': 2, 'facecolor': 'w'})

    axs[0].text(0, 1.2, 'Device name: ' + device_name[device_id], color='black', transform=axs[0].transAxes,
                fontsize=20)
    axs[0].text(0, 1.05,
                'ENERGY CONSUMPTION (' + track_day + '): ' + str(get_energy_consumption(df_energy) / 1000) + ' kWh',
                color='black', transform=axs[0].transAxes, fontsize=20)

    axs[0].legend(p1 + p2, labs, loc=0)

    fig.savefig(f'{bg_dir}/chart_{device_name[device_id]}.png')
    plt.close()
    # plt.show()


# Pie chart
def func(pct, allvalues):
    absolute = pct / 100. * np.sum(allvalues)
    return "{:.1f}%\n({:.3f} kWh)".format(pct, absolute)


def export_pie_chart_energy_consumption(bg_dir, user_id, track_day, device_list, energy_list, saving_percent):
    os.makedirs(bg_dir + username[user_id] + '/Energy Pie Chart/', exist_ok=True)

    # Wedge properties
    wp = {'linewidth': 2, 'edgecolor': "black"}

    # Creating plot
    fig, ax = plt.subplots(figsize=(15, 10))
    wedges, texts, autotexts = ax.pie(energy_list,
                                      autopct=lambda pct: func(pct, energy_list),
                                      # explode = explode,
                                      labels=device_list,
                                      # shadow = True,
                                      # colors = colors,
                                      startangle=90,
                                      wedgeprops=wp,
                                      textprops={'fontsize': 14}
                                      )

    # Adding legend
    ax.legend(wedges, device_list,
              title="Device List",
              loc="center left",
              bbox_to_anchor=(1, 0, 0.5, 1))

    plt.setp(autotexts, size=12, weight="bold")
    ax.set_title(
        'Total Energy Consumption ({}): {:.2f} kWh \n Saving energy: {:2f}%'.format(track_day, np.sum(energy_list),
                                                                                    saving_percent), fontsize=12)

    plt.savefig(bg_dir + username[user_id] + '/Energy Pie Chart/EnergyPieChart_' + track_day + '.png')
    # plt.show()
    plt.close()


# Energy consumption and Working time
def export_energy_consumption_and_working_time_chart(bg_dir, user_id, device_list, track_day):
    os.makedirs(bg_dir + username[user_id] + '/Last 3 days bar chart/', exist_ok=True)

    date1 = pd.to_datetime(track_day)
    date2 = date1 - datetime.timedelta(days=1)
    date3 = date1 - datetime.timedelta(days=2)

    track_day_2 = '{}-{:02d}-{:02d}'.format(date2.year, date2.month, date2.day)
    track_day_3 = '{}-{:02d}-{:02d}'.format(date3.year, date3.month, date3.day)

    e1 = []
    e2 = []
    e3 = []

    t1 = []
    t2 = []
    t3 = []

    label = []

    for device_id in device_list:
        df_energy_1 = extract_energy_df(bg_dir, user_id, device_id, track_day)
        df_energy_2 = extract_energy_df(bg_dir, user_id, device_id, track_day_2)
        df_energy_3 = extract_energy_df(bg_dir, user_id, device_id, track_day_3)

        ec1 = get_energy_consumption(df_energy_1) / 1000
        ec2 = get_energy_consumption(df_energy_2) / 1000
        ec3 = get_energy_consumption(df_energy_3) / 1000

        wt1 = get_working_time(df_energy_1) / 3600
        wt2 = get_working_time(df_energy_2) / 3600
        wt3 = get_working_time(df_energy_3) / 3600

        if not np.isnan(ec1):
            e1.append(ec1)
        else:
            e1.append(0)

        if not np.isnan(ec2):
            e2.append(ec2)
        else:
            e2.append(0)

        if not np.isnan(ec3):
            e3.append(ec3)
        else:
            e3.append(0)

        if not np.isnan(wt1):
            t1.append(wt1)
        else:
            t1.append(0)

        if not np.isnan(wt2):
            t2.append(wt2)
        else:
            t2.append(0)

        if not np.isnan(wt3):
            t3.append(wt3)
        else:
            t3.append(0)

        label.append(device_name[device_id])

    df_energy_consumption = pd.DataFrame({
        'Device Name': label,
        track_day_3: e3,
        track_day_2: e2,
        track_day: e1
    })

    df_working_time = pd.DataFrame({
        'Device Name': label,
        track_day_3: t3,
        track_day_2: t2,
        track_day: t1
    })

    df_energy_consumption = pd.melt(df_energy_consumption, id_vars="Device Name", var_name="Track Day",
                                    value_name='Energy Consumption (kWh)')
    df_working_time = pd.melt(df_working_time, id_vars="Device Name", var_name="Track Day",
                              value_name='Working Time (Hours)')

    fig, axs = plt.subplots(2, 1, sharex=True, figsize=(15, 10))
    fig.subplots_adjust(hspace=0.05)

    g = sns.barplot(ax=axs[0], x='Device Name', y='Energy Consumption (kWh)', hue='Track Day',
                    data=df_energy_consumption)
    g.set(xlabel=None)
    ax = axs[0]
    for p in ax.patches:
        axs[0].text(p.get_x() + p.get_width() / 2., p.get_height(), '%.2f' % p.get_height(),
                    fontsize=12, color='red', ha='center', va='bottom')

    sns.barplot(ax=axs[1], x='Device Name', y='Working Time (Hours)', hue='Track Day', data=df_working_time)
    ax = axs[1]
    for p in ax.patches:
        axs[1].text(p.get_x() + p.get_width() / 2., p.get_height(), '%.2f' % p.get_height(),
                    fontsize=12, color='red', ha='center', va='bottom')

    # axs[0].text(-0.5, df_energy_consumption['Energy Consumption (kWh)'].max() + 1.5, 'THE LAST 3 DAYS CHART OF ENERGY CONSUMPTION AND WORKING TIME', fontsize=12)
    plt.xticks(rotation=45)

    plt.savefig(bg_dir + username[user_id] + '/Last 3 days bar chart/Last3daysBarChart_' + track_day + '.png')

    # plt.show()
    plt.close()
