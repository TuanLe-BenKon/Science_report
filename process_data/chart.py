import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import datetime
import pandas as pd

from process_data.utils import *


# Draw chart
# Basic chart
def export_chart(bg_dir, device_name, device_id, df_sensor, df_energy, df_activities: pd.DataFrame, date):
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
