import os
import csv
import requests
import boto3
import psycopg2
import json
from datetime import datetime
import countries_lat_lon

CLIMATE_DATA_BUCKET = 'climate.ml'

appid = os.environ['openweather_key']
USERNAME = os.environ['db_username']
PASS = os.environ['db_pass']
bbox = countries_lat_lon.bbox

bbox_iran = bbox.get('Iran')

baseurl = 'http://api.openweathermap.org/data/2.5/'
yql_url = baseurl + 'box/city?bbox=' + bbox_iran
yql_url += '&units=metric' + '&appid=' + appid + "&format=json"
s3_client = boto3.client('s3')

DB_HOST_NAME = 'climatets.cjd6mojt7syg.us-east-1.rds.amazonaws.com'


def put_data_in_s3(data, filename):
    """
    writes json data in AWS S3.

    :param list data: JSON serializable data
    :param str filename: name of the file
    """
    s3_client.put_object(Body=data, Bucket=CLIMATE_DATA_BUCKET, Key=filename)

def write_wind_data_to_csv(data):
    """
    extracts wind data from the given data and writes as a csv file.

    :param list data: a list of dictionaries each having the following structure

    ::
        {..., 'wind': {'speed': <float>, 'deg': <float>},
              'dt': <int>, 'coord': {'Lat': <float>, 'Lon': <float>}, ...}
    """

    csvfile = open('/tmp/wind_data.csv', 'wb')
    writer = csv.writer(csvfile, delimiter=',')
    for data_dict in data:
        v = data_dict.get('wind').get('speed')
        deg = data_dict.get('wind').get('deg')
        utc_time = data_dict.get('dt')
        lat, lon = data_dict.get('coord').get('Lat'), data_dict.get('coord').get('Lon')
        writer.writerow([v, deg, utc_time, lat, lon])

def write_temperature_data_to_csv(data):
    """
    extracts temperature data from the given data and writes as a csv file.

    :param list data: a list of dictionaries each having the following structure

    ::
        {..., 'main': {'temp': <float>, ...},
              'dt': <int>, 'coord': {'Lat': <float>, 'Lon': <float>}, ...}
    """

    csvfile = open('/tmp/temperature_data.csv', 'wb')
    writer = csv.writer(csvfile, delimiter=',')
    for data_dict in data:
        temp = data_dict.get('main').get('temp')
        utc_time = data_dict.get('dt')
        lat, lon = data_dict.get('coord').get('Lat'), data_dict.get('coord').get('Lon')
        writer.writerow([temp, utc_time, lat, lon])

def write_pressure_data_to_csv(data):
    """
    extracts pressure data from the given data and writes as a csv file.

    :param list data: a list of dictionaries each having the following structure

    ::
        {..., 'wind': {'pressure': <float>, ...},
              'dt': <int>, 'coord': {'Lat': <float>, 'Lon': <float>}, ...}
    """

    csvfile = open('/tmp/pressure_data.csv', 'wb')
    writer = csv.writer(csvfile, delimiter=',')
    for data_dict in data:
        p = data_dict.get('main').get('pressure')
        utc_time = data_dict.get('dt')
        lat, lon = data_dict.get('coord').get('Lat'), data_dict.get('coord').get('Lon')
        writer.writerow([p, utc_time, lat, lon])


def write_humidity_data_to_csv(data):
    """
    extracts humidity data from the given data and writes as a csv file.

    :param list data: a list of dictionaries each having the following structure

    ::
        {..., 'main': {'humidity': <float>, ...},
              'dt': <int>, 'coord': {'Lat': <float>, 'Lon': <float>}, ...}
    """

    csvfile = open('/tmp/humidity_data.csv', 'wb')
    writer = csv.writer(csvfile, delimiter=',')
    for data_dict in data:
        humidity = data_dict.get('main').get('humidity')
        utc_time = data_dict.get('dt')
        lat, lon = data_dict.get('coord').get('Lat'), data_dict.get('coord').get('Lon')
        writer.writerow([humidity, utc_time, lat, lon])

def write_condition_data_to_csv(data):
    """
    extracts wind data from the given data and writes as a csv file.

    :param list data: a list of dictionaries each having the following structure

    ::
        {..., 'weather': {'description': <float>, ...},
              'dt': <int>, 'coord': {'Lat': <float>, 'Lon': <float>}, ...}
    """

    csvfile = open('/tmp/condition_data.csv', 'wb')
    writer = csv.writer(csvfile, delimiter=',')
    for data_dict in data:
        weather = data_dict.get('weather')
        if weather:
            _condition = weather[0].get('description', '')
            condition = _condition.replace(',', '')
        else:
            condition = ''
        utc_time = data_dict.get('dt')
        lat, lon = data_dict.get('coord').get('Lat'), data_dict.get('coord').get('Lon')
        writer.writerow([condition, utc_time, lat, lon])


def main(event, context):
    """
    scheduled function that requests data from openweathermap API and if the response
    is OK (200) it writes the content to AWS S3. After that it parses the data and extracts
    temperature, pressure, wind speed, humidty and condition and writes them in an AWS postgres
    database.
    """
    r = requests.get(yql_url)

    if r.status_code == 200:
        content = r.content
        t = datetime.now()
        filename = 'iran_' + t.strftime('%Y-%m-%dT%H-%M-%S') + '.json'
        put_data_in_s3(content, filename)

        data_dict = json.loads(content)
        data = data_dict.get('list')

        # start inserting data into timeseries database
        write_wind_data_to_csv(data)
        conn = psycopg2.connect(dbname='climateTSDB', host=DB_HOST_NAME,
                                user=USERNAME, password=PASS)
        cur = conn.cursor()
        f = open('/tmp/wind_data.csv', 'r')
        cur.copy_from(f, 'windts', sep=',', columns=['speed', 'degree',
            'utc_time', 'latitude', 'longitude'])
        conn.commit()
        try:
            os.remove('/tmp/wind_data.csv')
        except OSError:
            pass
        cur.close()

        write_temperature_data_to_csv(data)
        conn = psycopg2.connect(dbname='climateTSDB', host=DB_HOST_NAME,
                                user=USERNAME, password=PASS)
        cur = conn.cursor()
        f = open('/tmp/temperature_data.csv', 'r')
        cur.copy_from(f, 'temperaturets', sep=',', columns=['temperature'
            ,'utc_time', 'latitude', 'longitude'])
        conn.commit()
        try:
            os.remove('/tmp/temperature_data.csv')
        except OSError:
            pass
        cur.close()

        write_pressure_data_to_csv(data)
        conn = psycopg2.connect(dbname='climateTSDB', host=DB_HOST_NAME,
                                user=USERNAME, password=PASS)
        cur = conn.cursor()
        f = open('/tmp/pressure_data.csv', 'r')
        cur.copy_from(f, 'pressurets', sep=',', columns=['pressure'
            ,'utc_time', 'latitude', 'longitude'])
        conn.commit()
        try:
            os.remove('/tmp/pressure_data.csv')
        except OSError:
            pass
        cur.close()

        write_humidity_data_to_csv(data)
        conn = psycopg2.connect(dbname='climateTSDB', host=DB_HOST_NAME,
                                user=USERNAME, password=PASS)
        cur = conn.cursor()
        f = open('/tmp/humidity_data.csv', 'r')
        cur.copy_from(f, 'humidityts', sep=',', columns=['humidity'
            ,'utc_time', 'latitude', 'longitude'])
        conn.commit()
        try:
            os.remove('/tmp/humidity_data.csv')
        except OSError:
            pass
        cur.close()

        write_condition_data_to_csv(data)
        conn = psycopg2.connect(dbname='climateTSDB', host=DB_HOST_NAME,
                                user=USERNAME, password=PASS)
        cur = conn.cursor()
        f = open('/tmp/condition_data.csv', 'r')
        cur.copy_from(f, 'conditionts', sep=',', columns=['condition'
            ,'utc_time', 'latitude', 'longitude'])
        conn.commit()
        try:
            os.remove('/tmp/condition_data.csv')
        except OSError:
            pass
        cur.close()
