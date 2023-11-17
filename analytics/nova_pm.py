import os
import csv
import io

import logging
import datetime
import argparse

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

try:
    import serial
except:
    print("Python serial library required:")
    print("\tapt-get install python3-serial")
    raise

LOG_FORMAT = "%(asctime)-15s %(levelname)-8s %(message)s"


def save_plot(filename):
    # Read the CSV file
    df = pd.read_csv(filename, parse_dates=['date'])

    # Parse the PM10 and PM2.5 columns as floats
    df['PM10'] = pd.to_numeric(df['PM10'])
    df['PM2.5'] = pd.to_numeric(df['PM2.5'])

    # Plot the PM10 and PM2.5 vs datetime
    plt.plot(df['date'], df['PM10'], label='PM10')
    plt.plot(df['date'], df['PM2.5'], label='PM2.5')

    # Add a legend
    plt.legend()

    # Save the plot to an image
    plt.savefig(f'{os.path.dirname(filename)}/plot.png')

def append_csv(filename, field_names, row_dict, save_image=True):
    file_exists = os.path.isfile(filename)
    with io.open(filename, 'a', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, delimiter=',',
                                lineterminator='\n',
                                fieldnames=field_names)

        if not file_exists:
            writer.writeheader()
        writer.writerow(row_dict)
    if save_image:
        save_plot(filename)

def read_nova_dust_sensor(device='/dev/ttyUSB0'):
    dev = serial.Serial(device, 9600)

    if not dev.isOpen():
        dev.open()

    msg = dev.read(10)

    assert msg[0] == ord(b'\xaa')
    assert msg[1] == ord(b'\xc0')
    assert msg[9] == ord(b'\xab')
    pm25 = (msg[3]*256 + msg[2]) / 10.0
    pm10 = (msg[5]*256 + msg[4]) / 10.0
    checksum = sum(v for v in msg[2:8]) % 256
    assert checksum==msg[8]
    return {'PM10': pm10, 'PM2.5': pm25}

def main():
    logging.basicConfig(format=LOG_FORMAT, level=logging.INFO)

    parser = argparse.ArgumentParser(description="Read data from Nova PM sensor.")
    parser.add_argument('--device', default='/dev/ttyUSB0', 
            help='Device file of connected USB RS232 Nova PM sensor')
    parser.add_argument('--csv', default=None, help='Append results to csv, you can use year, month, day in format')
    args = parser.parse_args()

    data = read_nova_dust_sensor(args.device)
    logging.info('PM10=% 3.1f ug/m^3 PM2.5=% 3.1f ug/m^3', data['PM10'], data['PM2.5'])

    if args.csv:
        field_list = ['date', 'PM10', 'PM2.5']
        today = datetime.datetime.today()

        data['date'] = today.strftime('%Y-%m-%d %H:%M:%S')
        csv_file = args.csv % {'year': today.year, 'month': '%02d' %today.month, 'day': '%02d' % today.day,}

        append_csv(csv_file, field_list, data)

if __name__ == '__main__':
    main()
